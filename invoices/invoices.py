from pymongo import MongoClient
from minio import Minio
from datetime import datetime
import json
import os
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
import pathlib
from io import BytesIO
from weasyprint import HTML
import tempfile

# Load environment variables
load_dotenv()

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "mydatabase")
COLLECTION_NAME = "customer_bills"

# MinIO connection
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "invoices")

# Setup Jinja2 environment
template_dir = pathlib.Path(__file__).parent / "templates"
env = Environment(loader=FileSystemLoader(template_dir))
template = env.get_template("invoice_template.html")


def connect_to_mongodb():
    """Connect to MongoDB and return the collection."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION_NAME]


def connect_to_minio():
    """Connect to MinIO and ensure the bucket exists."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,  # Set to True if using HTTPS
    )

    # Create bucket if it doesn't exist
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    return client


def generate_invoice(bill):
    """Generate an invoice from a customer bill."""
    invoice = {
        "invoice_id": str(bill["_id"]),
        "customer_id": str(bill["customer_id"]),
        "customer_name": bill["customerName"],
        "subscription_type": bill["subscriptionType"],
        "billing_date": datetime.now().strftime("%Y-%m-%d"),
        "charges": {
            "total": bill["total_charge"],
            "voice": bill["voice_charge"],
            "sms": bill["sms_charge"],
            "data": bill["data_charge"],
        },
        "usage": {
            "voice_minutes": bill["voice_usage"],
            "sms_count": bill["sms_usage"],
            "data_mb": bill["data_usage"],
        },
    }
    return invoice


def store_invoice_in_minio(minio_client, invoice):
    """Store the invoice in MinIO as both HTML and PDF."""
    # Render the HTML template
    html_content = template.render(invoice=invoice)

    # Store HTML version
    html_object_name = f"invoice_{invoice['invoice_id']}.html"
    html_data = BytesIO(html_content.encode("utf-8"))
    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=html_object_name,
        data=html_data,
        length=len(html_content.encode("utf-8")),
        content_type="text/html",
    )

    # Convert to PDF using a temporary file
    with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as temp_html:
        temp_html.write(html_content.encode("utf-8"))
        temp_html_path = temp_html.name

    try:
        # Generate PDF from the temporary HTML file
        pdf = HTML(filename=temp_html_path).write_pdf()

        # Store PDF version
        pdf_object_name = f"invoice_{invoice['invoice_id']}.pdf"
        pdf_data = BytesIO(pdf)
        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=pdf_object_name,
            data=pdf_data,
            length=len(pdf),
            content_type="application/pdf",
        )
    finally:
        # Clean up the temporary file
        os.unlink(temp_html_path)

    return html_object_name, pdf_object_name


def process_invoices():
    """Main function to process all customer bills and generate invoices."""
    try:
        # Connect to MongoDB
        collection = connect_to_mongodb()

        # Connect to MinIO
        minio_client = connect_to_minio()

        # Process all bills
        bills = collection.find({})
        processed_count = 0

        for bill in bills:
            # Generate invoice
            invoice = generate_invoice(bill)

            # Store in MinIO
            html_object_name, pdf_object_name = store_invoice_in_minio(
                minio_client, invoice
            )
            processed_count += 1

            print(
                f"Processed invoice for {invoice['customer_name']} - HTML Stored as {html_object_name}, PDF Stored as {pdf_object_name}"
            )

        print(f"Successfully processed {processed_count} invoices")

    except Exception as e:
        print(f"Error processing invoices: {str(e)}")


if __name__ == "__main__":
    process_invoices()
