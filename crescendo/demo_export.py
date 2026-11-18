"""Capture the real Tk widget on X11 as PNG, using only the standard library.

Run on an owned display of at least 1920x1280 pixels. The application is never
rendered off-screen or reconstructed with a separate plotting library.
"""
import ctypes
import ctypes.util
import json
import math
import os
import struct
import tempfile
import zlib


def capture_png(widget, path):
    """Read a mapped X11 TrueColor window and write a lossless RGB PNG."""
    xlib = ctypes.CDLL(ctypes.util.find_library("X11"))
    xlib.XOpenDisplay.argtypes = [ctypes.c_char_p]
    xlib.XOpenDisplay.restype = ctypes.c_void_p
    xlib.XCloseDisplay.argtypes = [ctypes.c_void_p]
    xlib.XGetImage.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.c_int, ctypes.c_int,
                               ctypes.c_uint, ctypes.c_uint, ctypes.c_ulong, ctypes.c_int]
    xlib.XGetImage.restype = ctypes.c_void_p
    xlib.XDestroyImage.argtypes = [ctypes.c_void_p]
    class XImage(ctypes.Structure):
        _fields_ = [(name, ctypes.c_int) for name in ("width", "height", "xoffset", "format")] + [
            ("data", ctypes.c_void_p)] + [(name, ctypes.c_int) for name in (
                "byte_order", "bitmap_unit", "bitmap_bit_order", "bitmap_pad", "depth", "bytes_per_line", "bits_per_pixel")] + [
            ("red_mask", ctypes.c_ulong), ("green_mask", ctypes.c_ulong), ("blue_mask", ctypes.c_ulong)]
    display = xlib.XOpenDisplay(os.environ.get("DISPLAY", "").encode("ascii"))
    if not display:
        raise RuntimeError("A visible X11 display is required for PNG capture")
    handle = None
    try:
        width, height = widget.winfo_width(), widget.winfo_height()
        handle = xlib.XGetImage(display, widget.winfo_id(), 0, 0, width, height, 0xffffffff, 2)
        if not handle:
            raise RuntimeError("Cannot capture the mapped Tk window")
        image = ctypes.cast(handle, ctypes.POINTER(XImage)).contents
        if image.bits_per_pixel not in (24, 32) or (image.red_mask, image.green_mask, image.blue_mask) != (0xff0000, 0xff00, 0xff):
            raise RuntimeError("PNG capture requires a 24-bit TrueColor X11 display")
        raw = ctypes.string_at(image.data, image.bytes_per_line * height)
        stride = image.bits_per_pixel // 8
        channels = (2, 1, 0) if image.byte_order == 0 else (stride-3, stride-2, stride-1)
        encoded = bytearray()
        for y in range(height):
            row = raw[y*image.bytes_per_line:y*image.bytes_per_line+width*stride]
            rgb = bytearray(width*3)
            for channel, source in enumerate(channels):
                rgb[channel::3] = row[source::stride]
            encoded.append(0)
            encoded.extend(rgb)
        def chunk(kind, data):
            return struct.pack("!I", len(data)) + kind + data + struct.pack("!I", zlib.crc32(kind+data) & 0xffffffff)
        with open(path, "wb") as stream:
            stream.write(b"\x89PNG\r\n\x1a\n")
            stream.write(chunk(b"IHDR", struct.pack("!2I5B", width, height, 8, 2, 0, 0, 0)))
            stream.write(chunk(b"IDAT", zlib.compress(bytes(encoded), 6)))
            stream.write(chunk(b"IEND", b""))
    finally:
        if handle:
            xlib.XDestroyImage(handle)
        xlib.XCloseDisplay(display)


def export_demo(directory, seed=2045, frames=48, generations=12, population=64):
    if not 1 <= frames <= 360:
        raise ValueError("frames must be between 1 and 360")
    from crescendo.demo_main import prepare_environment, build_config
    from crescendo.demo_service import DemoService, three_objective_dataset
    from crescendo import app
    from crescendo_engine import gui
    import shutil
    directory = os.path.abspath(directory)
    os.makedirs(directory, exist_ok=True)
    # Scratch files stay with the explicitly requested export directory.
    base, _ = prepare_environment(tempfile.mkdtemp(prefix=".demo-", dir=directory))
    root = None
    window = None
    try:
        app.install()
        config = build_config(base)
        service = DemoService(workdir=os.path.join(base, "service"), config=config, latency=0,
                              generations=generations, population=population, seed=seed, history_runs=0)
        document = service.three_objective_demo()
        with open(os.path.join(directory, "pareto-3d.json"), "w", encoding="utf-8") as stream:
            json.dump(document, stream, separators=(",", ":"), ensure_ascii=True)
        root = gui.tk.Tk()
        if root.winfo_screenwidth() < 1920 or root.winfo_screenheight() < 1280:
            raise ValueError("HD capture requires an X11 display of at least 1920x1280 pixels")
        root.tk.call("tk", "scaling", 2.0)
        root.geometry("1920x1280+0+0")
        window = gui.OptimizerGui(root, service)
        window.pareto_3d_bounds = document["axis_bounds"]
        root.geometry("1920x1280+0+0")
        for generation in range(generations):
            window.display_dataset = three_objective_dataset(document, generation)
            window.pareto_ids = set(document["frames"][generation]["pareto_ids"])
            window.render_results()
            window.render_pareto()
            window.show_page("Pareto Trade-offs")
            plot = window.pareto_3d
            plot.canvas.configure(height=720)
            window._set_status("Synthetic demo · Generation {} / {}".format(generation+1, generations))
            root.update()
            plot.redraw()
            root.update_idletasks()
            capture_png(plot, os.path.join(directory, "generation-{:03d}.png".format(generation)))
        capture_png(root, os.path.join(directory, "pareto-3d-ui.png"))
        initial = plot.yaw
        for index in range(frames):
            plot.yaw = initial + 2*math.pi*index/frames
            plot.redraw()
            root.update_idletasks()
            capture_png(plot, os.path.join(directory, "rotation-{:03d}.png".format(index)))
        # Dedicated 16:9 canvas, no app chrome, same retained renderer/data.
        from crescendo_engine.pareto_3d import Pareto3DPlot, triple_points
        hero = gui.tk.Toplevel(root)
        hero.overrideredirect(True)
        hero.geometry("1920x1080+0+0")
        hero_plot = Pareto3DPlot(hero, window, document['objectives'], bounds=document['axis_bounds'])
        hero_plot.pack(fill="both", expand=True)
        hero_plot.configure(padding=0)
        for child in hero_plot.winfo_children():
            if child is not hero_plot.canvas:
                child.pack_forget()
        hero_plot.render(triple_points(window.display_dataset['candidates'], document['objectives'], window.pareto_ids))
        root.update()
        hero_plot.redraw()
        root.update_idletasks()
        capture_png(hero_plot.canvas, os.path.join(directory, "pareto-3d-hero.png"))
        hero.destroy()
        print("Exported {} rotation frames, {} generation frames and JSON to {}".format(frames, generations, directory))
        return 0
    finally:
        if window is not None:
            window.closing = True
            window.tasks.close()
        if root is not None:
            root.destroy()
        shutil.rmtree(base, ignore_errors=True)
