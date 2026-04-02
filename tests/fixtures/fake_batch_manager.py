class FakeBatchManager:
    def __init__(self):
        self.batches = {}
        self.legs = {}

    def seed_batch(self, batch_id, batch_data, legs):
        self.batches[batch_id] = batch_data
        self.legs[batch_id] = legs

    def get_batch(self, batch_id):
        return self.batches.get(batch_id)

    def get_batch_legs(self, batch_id):
        return self.legs.get(batch_id, [])

    def update_leg_status(
        self,
        batch_id,
        leg_index,
        status,
        bet_id=None,
        raw_response=None,
        error_text=None,
    ):
        for leg in self.legs.get(batch_id, []):
            if int(leg.get("leg_index", -1)) == int(leg_index):
                leg["status"] = status
                if bet_id is not None:
                    leg["bet_id"] = bet_id
                if raw_response is not None:
                    leg["raw_response"] = raw_response
                if error_text is not None:
                    leg["error_text"] = error_text
                return

    def recompute_batch_status(self, batch_id):
        batch = self.batches.get(batch_id, {})
        legs = self.legs.get(batch_id, [])

        statuses = {str(l.get("status", "")).upper() for l in legs}

        if not legs:
            batch["status"] = "FAILED"
        elif statuses == {"MATCHED"}:
            batch["status"] = "EXECUTED"
        elif "FAILED" in statuses and "MATCHED" in statuses:
            batch["status"] = "PARTIAL"
        elif "FAILED" in statuses:
            batch["status"] = "FAILED"
        else:
            batch["status"] = "LIVE"

        self.batches[batch_id] = batch
        return batch

    def mark_batch_failed(self, batch_id, reason=""):
        batch = self.batches.setdefault(batch_id, {"batch_id": batch_id})
        batch["status"] = "FAILED"
        batch["reason"] = reason

    def get_open_batches(self):
        return [
            b for b in self.batches.values()
            if str(b.get("status", "")).upper()
            not in {"EXECUTED", "FAILED", "ROLLED_BACK", "CANCELLED"}
        ]

    def mark_batch_rollback_pending(self, batch_id, reason=""):
        batch = self.batches.setdefault(batch_id, {"batch_id": batch_id})
        batch["status"] = "ROLLBACK_PENDING"
        batch["reason"] = reason

    def update_batch_status(self, batch_id, status, notes=""):
        batch = self.batches.setdefault(batch_id, {"batch_id": batch_id})
        batch["status"] = status
        batch["notes"] = notes

    def release_runtime_artifacts(
        self,
        batch_id,
        duplication_guard=None,
        table_manager=None,
        pnl=0.0,
    ):
        return None