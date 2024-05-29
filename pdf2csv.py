import argparse
import os
import requests
from io import BytesIO
from urllib.parse import urlparse
from textractor import Textractor
from textractor.visualizers.entitylist import EntityList
from textractor.data.constants import TextractFeatures
from pdf2image import convert_from_bytes
from PIL import Image
#from IPython.display import display
from PyPDF2 import PdfReader, PdfWriter
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt
import hashlib
from pathlib import Path


def process_image_file(file_bytes):
    try:
        with Image.open(file_bytes) as image:
            extractor = Textractor(region_name='us-east-1')
            document = extractor.analyze_document(
                file_source=image,
                features=[TextractFeatures.TABLES],
                save_image=True
            )
            table = EntityList(document.tables[0])
            return table[0].to_pandas().to_csv(index=False, header=False)
    except Exception as e:
        print (e)
        return ""



def process_pdf_file(file_bytes, all_pages, selected_pages=None):
    extractor = Textractor(region_name='us-east-1')

    if not all_pages:
        if selected_pages is None:
            selected_pages = [0]  # Default to the first page

    pdf = PdfReader(file_bytes)
    selected_pages = set(selected_pages)  # Convert to a set for faster look-up

    csv_outputs = []
    page_images = []

    for i in range(len(pdf.pages)):
        if all_pages or i in selected_pages:
            page = pdf.pages[i]
            pdf_writer = PdfWriter()
            pdf_writer.add_page(page)
            # Write page to byte stream
            stream = BytesIO()
            pdf_writer.write(stream)
            stream.seek(0)
            # Convert byte stream to image
            page_image = convert_from_bytes(stream.getvalue())[0]
            page_images.append(page_image)

    @retry(stop=(stop_after_attempt(3)))
    def process_page(page_image):
        try:
           # Extract tables from pages
            document = extractor.start_document_analysis(
                file_source=page_image,
                features=[TextractFeatures.TABLES],
                save_image=True,
                s3_upload_path="s3://finstatementsja/textract-temp/"
            )
            table = EntityList(document.tables[0])
            return table[0].to_pandas().to_csv(index=False, header=False)
        except Exception as e:
          print(f"Error processing page: {e}")
          raise

    # Process all pages concurrently
    with ThreadPoolExecutor() as executor:
        csv_outputs = list(executor.map(process_page, page_images))

    return csv_outputs


def is_url(path):
    try:
        result = urlparse(path)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def main():
    parser = argparse.ArgumentParser(description="Extract tables from PDF or image files and convert them to CSV.")
    parser.add_argument("file", type=str, help="Path to the input PDF or image file or URL.")
    parser.add_argument("--all_pages", action="store_true", help="Extract tables from all pages of the PDF file.")
    parser.add_argument("--selected_pages", type=int, nargs="+", help="Specify the pages to extract tables from (0-indexed).")
    args = parser.parse_args()

    if is_url(args.file):
        response = requests.get(args.file)
        file_bytes = BytesIO(response.content)
        file_ext = os.path.splitext(args.file)[1].lower()
        file_name = os.path.basename(args.file).rsplit(".", 1)[0]
    else:
        file_ext = os.path.splitext(args.file)[1].lower()
        file_name = os.path.basename(args.file).rsplit(".", 1)[0]
        with open(args.file, "rb") as file:
            file_bytes = BytesIO(file.read())

    if file_ext == ".pdf":
        csv_outputs = process_pdf_file(file_bytes, args.all_pages, args.selected_pages)
    else:
        csv_outputs = [process_image_file(file_bytes)]

    for i, csv_output in enumerate(csv_outputs, 1):
        csv_output = csv_output.replace("\r", "")
        print(f"CSV Output {i}:")
        print(csv_output)
        print("\n")

        output_file_name = f"{file_name}_output_table_{i}.csv"
        with open(output_file_name, "w") as output_file:
            output_file.write(csv_output)


def process_file(file_path, all_pages=False, no_export=False, selected_pages=None):
    def generate_output_file_name(file_name, index):
        file_hash = f"{hashlib.md5((file_name).encode('utf-8')).hexdigest()}_{str(index)}"
        return f"csv_output/{file_hash}.csv"
    
    def read_existing_csv_files(file_name):
        file_hash = hashlib.md5((file_name).encode('utf-8')).hexdigest()
        csv_files = []
        try:
            for csv_file in Path("csv_output").glob(f"{file_hash}_*.csv"):
                with open(csv_file, "r") as f:
                    csv_files.append(f.read())
        except Exception as e:
            print (e)
        return csv_files

    if is_url(file_path):
        response = requests.get(file_path)
        file_bytes = BytesIO(response.content)
        file_ext = os.path.splitext(file_path)[1].lower()
        file_name = os.path.basename(file_path).rsplit(".", 1)[0]
    else:
        file_ext = os.path.splitext(file_path)[1].lower()
        file_name = os.path.basename(file_path).rsplit(".", 1)[0]
        print (file_path)
        with open(file_path, "rb") as file:
            file_bytes = BytesIO(file.read())
    
    if not os.path.exists("csv_output"):
        os.makedirs("csv_output")
    
    #don't do any more work if the PDF has already been seen
    existing_csvs = read_existing_csv_files(file_name)
    if existing_csvs:
        print(f"{'-' * 20}\nPDF EXISTS\n{'-' * 20}")
        return existing_csvs
    
    print(f"{'-' * 20}\nPDF DOES NOT EXIST\n{'-' * 20}")
    if file_ext == ".pdf":
        csv_outputs = process_pdf_file(file_bytes, all_pages, selected_pages)
    else:
        csv_outputs = [process_image_file(file_bytes)]

    for i, csv_output in enumerate(csv_outputs, 0):
        csv_output = csv_output.replace("\r", "")
        print(f"CSV Output {i + 1}:")
        print(csv_output)
        print("\n")

        output_file_name = generate_output_file_name(file_name, i)
        with open(output_file_name, "w") as output_file:
            output_file.write(csv_output)
       
    
    return csv_outputs

if __name__ == "__main__":
    main()
