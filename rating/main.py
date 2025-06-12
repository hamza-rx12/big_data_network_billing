from rating_processor import RatingProcessor
import logging


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    try:
        # Create and run the rating processor
        processor = RatingProcessor()
        processor.process_ratings()
        logger.info("Rating processing completed successfully")
    except Exception as e:
        logger.error(f"Error in rating processing: {str(e)}")
        raise
    finally:
        # Ensure the Spark session is closed
        if "processor" in locals():
            processor.close()


if __name__ == "__main__":
    main()
