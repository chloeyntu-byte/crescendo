#!/usr/bin/env python3
"""Grab an X11 screen (or window id) to PPM/PNG via libX11 ctypes. Usage: xshot.py OUT.png [--display :77] [--window 0xID]"""
import ctypes, ctypes.util, os, shutil, sys, subprocess, struct
FFMPEG = os.environ.get("FFMPEG") or shutil.which("ffmpeg") or "ffmpeg"
args = sys.argv[1:]; out = args[0]
disp = os.environ.get('DISPLAY', ':77'); win = None
if '--display' in args: disp = args[args.index('--display')+1]
if '--window' in args: win = int(args[args.index('--window')+1], 0)
X = ctypes.cdll.LoadLibrary(ctypes.util.find_library('X11'))
X.XOpenDisplay.restype = ctypes.c_void_p; X.XOpenDisplay.argtypes=[ctypes.c_char_p]
d = X.XOpenDisplay(disp.encode())
if not d: sys.exit('cannot open display')
X.XDefaultRootWindow.restype = ctypes.c_ulong; X.XDefaultRootWindow.argtypes=[ctypes.c_void_p]
root = win or X.XDefaultRootWindow(d)
class XWA(ctypes.Structure):
    _fields_=[('x',ctypes.c_int),('y',ctypes.c_int),('width',ctypes.c_int),('height',ctypes.c_int)]+[('pad',ctypes.c_char*256)]
wa = XWA(); X.XGetWindowAttributes.argtypes=[ctypes.c_void_p,ctypes.c_ulong,ctypes.c_void_p]
X.XGetWindowAttributes(d, root, ctypes.byref(wa))
w,h = wa.width, wa.height
X.XGetImage.restype = ctypes.c_void_p
X.XGetImage.argtypes=[ctypes.c_void_p,ctypes.c_ulong,ctypes.c_int,ctypes.c_int,ctypes.c_uint,ctypes.c_uint,ctypes.c_ulong,ctypes.c_int]
img = X.XGetImage(d, root, 0, 0, w, h, 0xFFFFFFFF, 2)
class XImage(ctypes.Structure):
    _fields_=[('width',ctypes.c_int),('height',ctypes.c_int),('xoffset',ctypes.c_int),('format',ctypes.c_int),('data',ctypes.c_void_p),('byte_order',ctypes.c_int),('bitmap_unit',ctypes.c_int),('bitmap_bit_order',ctypes.c_int),('bitmap_pad',ctypes.c_int),('depth',ctypes.c_int),('bytes_per_line',ctypes.c_int),('bits_per_pixel',ctypes.c_int)]
xi = ctypes.cast(img, ctypes.POINTER(XImage)).contents
raw = ctypes.string_at(xi.data, xi.bytes_per_line*h)
ppm = out + '.ppm'
with open(ppm,'wb') as f:
    f.write(b'P6\n%d %d\n255\n' % (w,h))
    bpl = xi.bytes_per_line
    for y in range(h):
        row = raw[y*bpl:y*bpl+w*4]
        rgb = bytearray(w*3); rgb[0::3]=row[2::4]; rgb[1::3]=row[1::4]; rgb[2::3]=row[0::4]
        f.write(rgb)
subprocess.check_call([FFMPEG,'-loglevel','error','-y','-i',ppm,out]); os.remove(ppm)
print(out, w, h)
