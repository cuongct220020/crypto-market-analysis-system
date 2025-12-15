

def validate_block_range(range_start_incl: int, range_end_incl: int) -> None:
    """
    Validate a block range for processing.
    
    Args:
        range_start_incl: The inclusive start block number.
        range_end_incl: The inclusive end block number.
    
    Raises:
        ValueError: If the block range is invalid.
    """
    validate_block_number(range_start_incl)
    validate_block_number(range_end_incl)

    if range_end_incl < range_start_incl:
        raise ValueError(
            f"range_end ({range_end_incl}) must be greater than or equal to range_start ({range_start_incl})"
        )


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