
import glob
import os

target_dir = '/Volumes/XML Auto Import'
pattern1 = os.path.join(target_dir, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_*.Out")
pattern2 = os.path.join(target_dir, "*.Out")

print(f"Testing pattern: {pattern1}")
files1 = glob.glob(pattern1)
print(f"Found {len(files1)} files.")
for f in files1:
    print(f" - {f}")

print(f"\nTesting pattern: {pattern2}")
files2 = glob.glob(pattern2)
print(f"Found {len(files2)} files.")
for f in files2[:5]:
    print(f" - {f}")
