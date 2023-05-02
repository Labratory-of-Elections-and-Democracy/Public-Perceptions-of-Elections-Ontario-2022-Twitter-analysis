import os
import shutil
import time

from tqdm import tqdm, trange


def copy_the_files(origin, destination):
    while True:
        for src_dir, dirs, files in os.walk(origin):
            dst_dir = src_dir.replace(origin, destination, 1)
            if not os.path.exists(dst_dir):
                os.makedirs(dst_dir)
            for file_ in tqdm(files):
                src_file = os.path.join(src_dir, file_)
                dst_file = os.path.join(dst_dir, file_)
                if os.path.exists(dst_file):
                    # in case of the src and dst are the same file
                    if os.path.samefile(src_file, dst_file):
                        continue
                    os.remove(dst_file)
                shutil.copy(src_file, dst_dir)
        t = 15 * 60
        print(f"Waiting for {t} seconds")
        time.sleep(1)
        for _ in trange(t):
            time.sleep(1)


if __name__ == '__main__':
    copy_the_files(
        "G:/LED_LAB/HollyMaddyEOProject/",
        "C:/Users/seang/Dropbox/Elections Ontario Project/Maddy Word Analysis/CSV Outfiles From Sean (In Progress)/HollyMaddyEOProject/"
    )
    while False:
        try:
            copy_the_files(
                "G:/LED_LAB/HollyMaddyEOProject/",
                "C:/Users/seang/Dropbox/Elections Ontario Project/Maddy Word Analysis/CSV Outfiles From Sean (In Progress)/HollyMaddyEOProject/"
            )
        except:
            t = 1 * 60
            print(f"Waiting for {t} seconds")
            time.sleep(1)
            for _ in trange(t):
                time.sleep(1)
