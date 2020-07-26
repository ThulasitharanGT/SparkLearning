#!/usr/bin/env python3

with open('cat.jpg', 'rb') as cat_picture:
    cat_picture.seek(2)
    cat_picture.read(4)
    print(cat_picture.tell())
    print(cat_picture.mode)
