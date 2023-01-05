"""
kicks off Step Function executions
"""
import json
import logging
import random
import os
import json

logger = logging.getLogger(__name__)


def lambda_handler(event, _):
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(log_level)
    logger.info(f"LOG_LEVEL: {log_level}")
    logger.info(json.dumps(event))

    return {'randomNumber': random.randint(0, 100)}
