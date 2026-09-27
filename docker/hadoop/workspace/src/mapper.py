def mapper(row):
    """
    Convert review record into key-value pair

    key:
        appid

    value:
        review information
    """

    return (
        row["appid"],
        {
            "game_name": row["game_name"],
            "voted_up": row["voted_up"],
            "playtime_hours": row["playtime_hours"]
        }
    )