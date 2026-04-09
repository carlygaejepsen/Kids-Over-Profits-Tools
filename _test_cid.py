import re

_CID_OFFSET = 29

def decode_cid_chars(text):
    if "(cid:" not in text:
        return text
    def _replace(m):
        char_code = int(m.group(1)) + _CID_OFFSET
        return chr(char_code) if 32 <= char_code <= 126 else ""
    return re.sub(r"\(cid:(\d+)\)", _replace, text)

raw = "(cid:53)(cid:53)(cid:72)(cid:86)(cid:76)(cid:71)(cid:72)(cid:81)(cid:87)(cid:76)(cid:68)(cid:79)(cid:79)(cid:3)(cid:55)(cid:85)(cid:72)(cid:68)(cid:87)(cid:80)(cid:72)(cid:81)(cid:87)(cid:87)(cid:3)(cid:44)(cid:81)(cid:86)(cid:83)(cid:72)(cid:70)(cid:87)(cid:76)(cid:82)(cid:81)(cid:81)(cid:3)(cid:38)(cid:75)(cid:72)(cid:70)(cid:78)(cid:79)(cid:76)(cid:86)(cid:87)"
decoded = decode_cid_chars(raw)
print("Decoded:", repr(decoded))

# Verify space and a few known chars
assert decoded[2] == ' ', f"Expected space at pos 2, got {repr(decoded[2])}"
assert decoded.startswith("RR"), f"Expected RR, got {decoded[:5]}"
assert "Treatment" in decoded
assert "Inspection" in decoded
assert "Checklist" in decoded
print("PASS")
