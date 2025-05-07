import glob
import os
import shutil
from pathlib import Path


def rename_files(directory, extension):
    """
    Rename files in the specified directory to sequential numbers with the given extension.
    Example: 0001.html, 0002.html, etc.
    """
    files = glob.glob(os.path.join(directory, f"*.{extension}"))
    files.sort()  # Sort to ensure consistent ordering

    for i, file_path in enumerate(files, 1):
        # Create the new filename with padded zeros
        new_filename = f"{i:04d}.{extension}"
        new_path = os.path.join(directory, new_filename)

        # Get original file name for logging
        original_filename = os.path.basename(file_path)

        # Create a backup of the original file
        backup_dir = os.path.join(directory, "backup")
        os.makedirs(backup_dir, exist_ok=True)
        backup_path = os.path.join(backup_dir, original_filename)
        shutil.copy2(file_path, backup_path)

        # Rename the file
        os.rename(file_path, new_path)
        print(f"Renamed: {original_filename} -> {new_filename}")


def main():
    # Define directories
    base_dir = Path(__file__).parent
    html_dir = base_dir / "evaluate" / "html"
    ground_truth_dir = base_dir / "evaluate" / "ground_truth"
    metadata_dir = base_dir / "evaluate" / "metadata"

    # Create backups and rename HTML files
    print("Renaming HTML files...")
    rename_files(html_dir, "html")

    # Create backups and rename JSON files in ground_truth
    print("Renaming ground truth JSON files...")
    rename_files(ground_truth_dir, "json")

    # Create backups and rename JSON files in metadata
    print("Renaming metadata JSON files...")
    rename_files(metadata_dir, "json")

    print("Renaming complete. Original files were backed up in the 'backup' folder.")


if __name__ == "__main__":
    main()
