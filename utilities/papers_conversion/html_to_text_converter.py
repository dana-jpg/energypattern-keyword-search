from pathlib import Path

from tqdm import tqdm
from bs4 import BeautifulSoup

def html_to_text(filename):
    global f
    # Assuming 'combined.html' is the file you created with 'cat'
    with open(filename, 'r', encoding='utf-8') as f:
        html_content = f.read()
    soup = BeautifulSoup(html_content, 'html.parser')  # Or 'lxml' for better parsing
    # Get all visible text from the entire document
    all_text = soup.get_text(separator='\n', strip=True)
    output_file = Path(filename).with_suffix('.txt')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(all_text)


folder_path = "../../metadata/papers"
html_files = Path(folder_path).glob("**/*.html")
for file in tqdm(html_files):
    tqdm.write(f"Processing {file}")
    html_to_text(file)
    Path(file).unlink()
