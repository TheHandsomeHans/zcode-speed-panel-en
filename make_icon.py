"""Generate app-icon.png: dark rounded background + gradient speedometer arc + needle"""
from PIL import Image, ImageDraw, ImageFilter
import math

S = 2048  # supersample, then downscale for anti-aliasing
A0 = 135  # start angle (degrees); PIL, like canvas, is clockwise with 0 at 3 o'clock
SWEEP = 270

img = Image.new("RGBA", (S, S), (0, 0, 0, 0))

# ---- vertical background gradient + rounded-corner clip ----
grad = Image.new("RGBA", (S, S))
gd = ImageDraw.Draw(grad)
top, bottom = (19, 26, 46), (9, 13, 24)
for y in range(S):
    t = y / S
    c = tuple(int(top[i] + (bottom[i] - top[i]) * t) for i in range(3)) + (255,)
    gd.line([(0, y), (S, y)], fill=c)

mask = Image.new("L", (S, S), 0)
md = ImageDraw.Draw(mask)
md.rounded_rectangle([16, 16, S - 16, S - 16], radius=int(S * 0.225), fill=255)
img.paste(grad, (0, 0), mask)

draw = ImageDraw.Draw(img)
cx, cy = S / 2, S * 0.52
R = S * 0.335
width = int(S * 0.062)

# ---- arc track ----
draw.arc([cx - R, cy - R, cx + R, cy + R], start=A0, end=A0 + SWEEP,
         fill=(255, 255, 255, 26), width=width)

# ---- gradient progress arc (colored segment by segment) ----
FRAC = 0.66
c1, c2 = (34, 211, 238), (16, 185, 129)  # cyan -> emerald
seg = 1.5
steps = int(SWEEP * FRAC / seg)
for i in range(steps):
    a = A0 + i * seg
    t = i / max(1, steps - 1)
    col = tuple(int(c1[k] + (c2[k] - c1[k]) * t) for k in range(3)) + (255,)
    draw.arc([cx - R, cy - R, cx + R, cy + R], start=a, end=a + seg + 0.4, fill=col, width=width)

# round caps at both arc ends
for a in (A0 + 0.6, A0 + SWEEP * FRAC - 0.6):
    ar = math.radians(a)
    ex, ey = cx + math.cos(ar) * R, cy + math.sin(ar) * R
    rr = width / 2 - 2
    draw.ellipse([ex - rr, ey - rr, ex + rr, ey + rr], fill=col if a != A0 + 0.6 else c1)

# ---- tick marks ----
for i in range(7):
    a = math.radians(A0 + SWEEP * i / 6)
    r1, r2 = R - width - S * 0.028, R - width - S * 0.012
    draw.line([(cx + math.cos(a) * r1, cy + math.sin(a) * r1),
               (cx + math.cos(a) * r2, cy + math.sin(a) * r2)],
              fill=(255, 255, 255, 60), width=int(S * 0.008))

# ---- needle (with glow) ----
glow = Image.new("RGBA", (S, S), (0, 0, 0, 0))
gdraw = ImageDraw.Draw(glow)
na = math.radians(A0 + SWEEP * FRAC)
needle_len = R - width * 1.1
nx, ny = cx + math.cos(na) * needle_len, cy + math.sin(na) * needle_len
tail = R * 0.22
tx, ty = cx - math.cos(na) * tail, cy - math.sin(na) * tail
gdraw.line([(tx, ty), (nx, ny)], fill=(34, 211, 238, 235), width=int(S * 0.017))
gdraw = ImageDraw.Draw(glow)
glow = glow.filter(ImageFilter.GaussianBlur(S * 0.012))
img.alpha_composite(glow)
draw = ImageDraw.Draw(img)
draw.line([(tx, ty), (nx, ny)], fill=(226, 250, 255, 255), width=int(S * 0.011))

# ---- center hub ----
hub = int(S * 0.052)
draw.ellipse([cx - hub, cy - hub, cx + hub, cy + hub], fill=(13, 20, 36, 255),
             outline=(34, 211, 238, 200), width=int(S * 0.007))
inner = int(hub * 0.42)
draw.ellipse([cx - inner, cy - inner, cx + inner, cy + inner], fill=(34, 211, 238, 255))

# ---- downscale output ----
img = img.resize((1024, 1024), Image.LANCZOS)
img.save("app-icon.png")
print("saved app-icon.png")
