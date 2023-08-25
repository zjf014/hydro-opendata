

def regen_box(bbox, resolution, offset):
    
    lx = bbox[0]
    rx = bbox[2]
    LLON = round(int(lx) + resolution * int((lx - int(lx)) / resolution + 0.5) + offset * (int(lx * 10) / 10 + offset - lx) / abs(int(lx * 10) // 10 + offset - lx + 0.0000001), 3)
    RLON = round(int(rx) + resolution * int((rx - int(rx)) / resolution + 0.5) - offset * (int(rx * 10) / 10 + offset - rx) / abs(int(rx * 10) // 10 + offset - rx + 0.0000001), 3)
    
    by = bbox[1]
    ty = bbox[3]
    BLAT = round(int(by) + resolution * int((by - int(by)) / resolution + 0.5) + offset * (int(by * 10) / 10 + offset - by) / abs(int(by * 10) // 10 + offset - by + 0.0000001), 3)
    TLAT = round(int(ty) + resolution * int((ty - int(ty)) / resolution + 0.5) - offset * (int(ty * 10) / 10 + offset - ty) / abs(int(ty * 10) // 10 + offset - ty + 0.0000001), 3)
    
    # print(LLON,BLAT,RLON,TLAT)
    
    return LLON,BLAT,RLON,TLAT