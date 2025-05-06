import os
import logging
from typing import Dict, Any, List
from apify_client import ApifyClient

logger = logging.getLogger(__name__)

class ApifyService:
    def __init__(self):
        self.apify_token = os.getenv("APIFY_TOKEN")
        if not self.apify_token:
            raise ValueError("APIFY_TOKEN environment variable is not set")
        
        self.client = ApifyClient(self.apify_token)

    async def run_actor(self, actor_id: str, run_input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run an Apify actor with the given input.
        
        Args:
            actor_id: ID of the Apify actor to run
            run_input: Input parameters for the actor
            
        Returns:
            Dict containing the run details including dataset_id
        """
        try:
            logger.info(f"Starting actor {actor_id}")
            logger.info(f"Run input configuration: {run_input}")

            run = self.client.actor(actor_id).call(run_input=run_input)
            logger.info(f"Run started with ID: {run.get('id')}")
            
            return run
        except Exception as e:
            logger.error(f"Error running actor {actor_id}: {str(e)}")
            raise Exception(f"Apify API error: {str(e)}")

    async def wait_for_run_to_finish(self, run_id: str) -> None:
        """
        Wait for a specific run to finish.
        
        Args:
            run_id: The ID of the run to wait for
        """
        while True:
            run = self.client.run(run_id).get()
            status = run.get("status")
            logger.info(f"Run status: {status}")
            
            if status in ["SUCCEEDED", "FAILED", "ABORTED"]:
                if status != "SUCCEEDED":
                    error_message = run.get("error", "Unknown error")
                    logger.error(f"Run failed with status: {status}, error: {error_message}")
                    raise Exception(f"Run failed with status: {status}, error: {error_message}")
                logger.info("Run completed successfully")
                break

    async def get_dataset_items(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Get items from a dataset.
        
        Args:
            dataset_id: ID of the dataset to fetch
            
        Returns:
            List of items from the dataset
        """
        logger.info(f"Fetching dataset items from dataset ID: {dataset_id}")
        try:
            dataset = self.client.dataset(dataset_id)
            items = list(dataset.list_items().items)
            
            logger.info(f"Retrieved {len(items)} items from dataset")
            
            if len(items) > 0:
                logger.info(f"Sample item structure: {items[0]}")
                logger.info(f"Available fields in first item: {list(items[0].keys())}")
            
            return items
        except Exception as e:
            logger.error(f"Error fetching dataset items: {str(e)}")
            raise 