import os, sys
sys.path.insert(0, "/home/user/Pickfair-nogui")
os.chdir(os.environ["HOME"])
import mini_gui
app = mini_gui.MiniPickfairGUI(force_simulation=True)
app.geometry("1400x900+0+0")
def show():
    try:
        tv = getattr(app, "tabview", None) or getattr(app, "tabs", None)
        for name in ("Roserpina", "🎯 Roserpina"):
            try:
                tv.set(name); break
            except Exception:
                pass
        # porta in vista la riga dei checkbox
        import gc, customtkinter as ctk
        for obj in gc.get_objects():
            if isinstance(obj, ctk.CTkScrollableFrame):
                try:
                    if str(obj.winfo_toplevel()) and obj.winfo_ismapped():
                        obj._parent_canvas.yview_moveto(float(os.environ.get("YPOS", "1.0")))
                except Exception:
                    pass
    except Exception as e:
        print("show error", e)
app.after(2500, show)
app.after(9000, app.destroy)
app.mainloop()
