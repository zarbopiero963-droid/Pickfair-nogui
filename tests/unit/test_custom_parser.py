"""Unit test del core PURO del Parser Personalizzato (services.custom_parser, P1).

Copre: trasformazioni, value-map, modello/validazione, motore di estrazione
(delimitatori tolleranti agli spazi, fixed, required/ready, ordine
transform→value-map, extract_scores, extract_between). Tutto fail-closed.
"""
import pytest

from services.custom_parser import (
    CustomParserDef,
    ExtractionResult,
    FieldRule,
    apply_parser,
    extract_between,
    extract_scores,
    extract_value,
    extract_value_traced,
    is_valid,
    skeleton,
    validate_parser_def,
)
from services.custom_parser import transforms, value_maps
from services.custom_parser.engine import (
    EXTRACT_END_NOT_FOUND,
    EXTRACT_FIXED,
    EXTRACT_NO_RULE,
    EXTRACT_OK,
    EXTRACT_START_NOT_FOUND,
)
from services.custom_parser.model import PARSER_TARGETS

pytestmark = pytest.mark.unit


# =========================================================
# transforms
# =========================================================
def test_score_to_over_basic():
    assert transforms.apply("6-0", "score_to_over") == "Over 6,5"
    assert transforms.apply("2-3", "score_to_over") == "Over 5,5"
    assert transforms.apply("0:0", "score_to_over") == "Over 0,5"
    assert transforms.apply("1 x 1", "score_to_over") == "Over 2,5"


def test_score_to_over_failclosed():
    # non interpretabile / implausibile => "" (mai una linea inventata)
    for bad in ("", "abc", "6", "999-999", "30-30", "31-0"):
        assert transforms.apply(bad, "score_to_over") == ""


def test_unknown_transform_returns_empty():
    assert transforms.apply("6-0", "nope") == ""
    assert not transforms.has_transform("nope")
    assert transforms.has_transform("score_to_over")
    assert transforms.available_transforms() == ["score_to_over"]


# =========================================================
# value_maps
# =========================================================
def test_bettype_builtin_back_lay():
    r = value_maps.registry()
    assert value_maps.resolve("BACK", "bettype", r) == "BACK"
    assert value_maps.resolve("punta", "bettype", r) == "BACK"
    assert value_maps.resolve("Lay", "bettype", r) == "LAY"
    assert value_maps.resolve("BANCA", "bettype", r) == "LAY"


def test_bettype_unmapped_and_ambiguous_letters_empty():
    # monolettera ambigua e alias ignoto => "" (mai indovinare il lato)
    for bad in ("B", "P", "", "x", "back lay"):
        assert value_maps.resolve(bad, "bettype") == ""


def test_resolve_unknown_map_empty():
    assert value_maps.resolve("BACK", "nope") == ""


def test_value_map_from_pairs_dedup_and_ambiguous():
    m = value_maps.value_map_from_pairs([
        ("GG", "Sì"), ("gg", "Sì"),           # stesso valore normalizzato => tenuto
        ("NO", "No"),
        ("X", "A"), ("x", "B"),               # ambiguo => scartato
        ("", "vuoto"), ("k", ""),             # vuoti => ignorati
        ("PH", "{HOME}"),                     # placeholder => ignorato
    ])
    assert m["gg"] == "Sì"
    assert m["no"] == "No"
    assert "x" not in m
    assert "" not in m
    assert "ph" not in m


def test_registry_is_defensive_copy():
    r = value_maps.registry()
    r["bettype"]["back"] = "HACKED"
    assert value_maps.resolve("back", "bettype") == "BACK"  # built-in intatto


# =========================================================
# modello + validazione
# =========================================================
def test_skeleton_is_valid():
    defn = skeleton("Canale X")
    assert is_valid(defn)
    assert validate_parser_def(defn) == []


def test_validate_rejects_unknown_target():
    defn = CustomParserDef(name="p", rules=[FieldRule(target="NopeCol")])
    errs = validate_parser_def(defn)
    assert any("campo non valido" in e for e in errs)


def test_validate_rejects_duplicate_target():
    defn = CustomParserDef(name="p", rules=[
        FieldRule(target="Price", start_after="q"),
        FieldRule(target="Price", start_after="z"),
    ])
    assert any("duplicato" in e for e in validate_parser_def(defn))


def test_validate_rejects_fixed_and_extraction_together():
    defn = CustomParserDef(name="p", rules=[
        FieldRule(target="Price", fixed_value="2.0", start_after="q"),
    ])
    assert any("fixed_value" in e for e in validate_parser_def(defn))


def test_validate_rejects_unknown_transform():
    defn = CustomParserDef(name="p", rules=[
        FieldRule(target="SelectionName", start_after="a", transform="nope"),
    ])
    assert any("trasformazione sconosciuta" in e for e in validate_parser_def(defn))


def test_validate_name_and_version_and_empty_rules():
    assert any("nome non vuoto" in e for e in validate_parser_def(CustomParserDef(name="")))
    assert any("spazi iniziali" in e for e in validate_parser_def(
        CustomParserDef(name=" x ", rules=[FieldRule(target="Price")])))
    assert any("almeno una regola" in e for e in validate_parser_def(CustomParserDef(name="p")))
    assert any("Versione" in e for e in validate_parser_def(
        CustomParserDef(name="p", version=0, rules=[FieldRule(target="Price")])))


def test_fieldrule_from_dict_required_tokens_and_extra_keys():
    r = FieldRule.from_dict({"target": "Price", "required": "sì", "extra": "ignored"})
    assert r.target == "Price" and r.required is True
    assert FieldRule.from_dict({"target": "Price", "required": "false"}).required is False
    with pytest.raises(ValueError):
        FieldRule.from_dict({"target": "Price", "required": "maybe"})
    with pytest.raises(ValueError):
        FieldRule.from_dict({"start_after": "x"})  # senza target


def test_parserdef_json_roundtrip():
    defn = skeleton("RT")
    back = CustomParserDef.from_json(defn.to_json())
    assert back.name == "RT"
    assert [r.target for r in back.rules] == [r.target for r in defn.rules]


def test_price_required_flag():
    assert skeleton().price_required() is True
    assert CustomParserDef(name="p", rules=[FieldRule(target="Price")]).price_required() is False


# =========================================================
# motore: estrazione a delimitatori
# =========================================================
def test_extract_fixed_value_wins():
    v, why = extract_value_traced("qualsiasi", FieldRule(target="Provider", fixed_value="TG"))
    assert v == "TG" and why == EXTRACT_FIXED


def test_extract_start_and_end():
    msg = "Evento: Milan v Inter | Quota 1.85 fine"
    r = FieldRule(target="Price", start_after="Quota", end_before="fine")
    assert extract_value(msg, r) == "1.85"


def test_extract_start_only_to_end_of_line():
    msg = "riga1 Quota 2.10\nriga2 altro"
    r = FieldRule(target="Price", start_after="Quota")
    assert extract_value(msg, r) == "2.10"  # fino a fine riga, non ingoia riga2


def test_extract_space_tolerant_delimiters():
    msg = "xx  Quota   :   3.4  yy END"
    r = FieldRule(target="Price", start_after="Quota :", end_before="END")
    # run di spazi interni flessibili + bordi ignorati
    assert extract_value(msg, r) == "3.4  yy"


def test_extract_start_not_found():
    v, why = extract_value_traced("nessun marker", FieldRule(target="Price", start_after="Quota"))
    assert v == "" and why == EXTRACT_START_NOT_FOUND


def test_extract_end_not_found_fails_closed():
    v, why = extract_value_traced(
        "Quota 1.9 senza terminatore",
        FieldRule(target="Price", start_after="Quota", end_before="ZZZ"))
    assert v == "" and why == EXTRACT_END_NOT_FOUND


def test_extract_no_rule_configured():
    v, why = extract_value_traced("testo", FieldRule(target="Price"))
    assert v == "" and why == EXTRACT_NO_RULE


def test_extract_empty_text_with_delims():
    v, why = extract_value_traced("", FieldRule(target="Price", start_after="Q"))
    assert v == "" and why == EXTRACT_START_NOT_FOUND


def test_extract_ok_reason():
    v, why = extract_value_traced("a X b", FieldRule(target="Price", start_after="a", end_before="b"))
    assert v == "X" and why == EXTRACT_OK


def test_extract_between_helper():
    assert extract_between("pre [ mid ] post", start_after="[", end_before="]") == "mid"
    assert extract_between("nope", start_after="[", end_before="]") == ""


# =========================================================
# motore: extract_scores (#325)
# =========================================================
def test_extract_scores_normalizes_and_dedups():
    region = "Risultati: 1-0, 01 - 0, 2 -1, 1-0"
    out = extract_scores("head " + region, start_after="Risultati:")
    assert out == ["1 - 0", "2 - 1"]


def test_extract_scores_ignores_decimals_and_times():
    # decimali/handicap e orari non devono produrre punteggi spuri
    assert extract_scores("linea 0-0.5 e ora 20:30 qui", start_after="linea") == []


def test_extract_scores_empty_region():
    assert extract_scores("x", start_after="MAI") == []


# =========================================================
# motore: apply_parser end-to-end
# =========================================================
def _parser_min():
    return CustomParserDef(name="p", rules=[
        FieldRule(target="Provider", fixed_value="TG_CUSTOM"),
        FieldRule(target="EventName", start_after="Match:", end_before="|", required=True),
        FieldRule(target="Price", start_after="Quota", end_before="\n", required=True),
        FieldRule(target="BetType", start_after="Lato", end_before="\n", value_map="bettype", required=True),
    ])


def test_apply_parser_ready_and_values():
    msg = "Match: Milan v Inter | mercato\nQuota 1.95\nLato BACK\n"
    res = apply_parser(_parser_min(), msg)
    assert isinstance(res, ExtractionResult)
    assert res.ready is True
    assert res.missing_required == []
    assert res.values["EventName"] == "Milan v Inter"
    assert res.values["Price"] == "1.95"
    assert res.values["BetType"] == "BACK"   # value-map applicata
    assert res.values["Provider"] == "TG_CUSTOM"


def test_apply_parser_not_ready_missing_required():
    msg = "Match: Milan v Inter | mercato\nLato BACK\n"   # manca Quota
    res = apply_parser(_parser_min(), msg)
    assert res.ready is False
    assert "Price" in res.missing_required


def test_apply_parser_unmapped_bettype_blocks():
    msg = "Match: A v B | m\nQuota 2.0\nLato FORSE\n"   # "FORSE" non mappato
    res = apply_parser(_parser_min(), msg)
    assert res.ready is False
    assert "BetType" in res.missing_required
    assert res.values["BetType"] == ""


def test_apply_parser_transform_then_value_map_order():
    defn = CustomParserDef(name="p", rules=[
        FieldRule(target="SelectionName", start_after="Score", end_before="\n", transform="score_to_over"),
    ])
    res = apply_parser(defn, "Score 6-0\n")
    assert res.values["SelectionName"] == "Over 6,5"


def test_as_values_covers_all_targets():
    res = apply_parser(_parser_min(), "Match: A v B | m\nQuota 2.0\nLato LAY\n")
    row = res.as_values()
    assert set(row.keys()) == set(PARSER_TARGETS)
    assert row["MarketName"] == ""       # nessuna regola => vuoto
    assert row["BetType"] == "LAY"
