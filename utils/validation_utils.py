

def validate_block_range(range_start_incl: int, range_end_incl: int) -> None:
    """
    Validate a block range for processing.
    """
    if range_start_incl < 0 or range_end_incl < 0:
        raise ValueError("range_start and range_end must be greater than or equal to 0")

    if range_end_incl < range_start_incl:
        raise ValueError("range_end must be greater than or equal to range_start")


def validate_block_number(block_number: int) -> None:
    """
    Validate a single block number.
    
    Args:
        block_number: The block number to validate, must be >= 0
    
    Raises:
        ValueError: If the block number is invalid
    """
    if block_number < 0:
        raise ValueError(f"Block number must be greater than or equal to 0, got {block_number}")