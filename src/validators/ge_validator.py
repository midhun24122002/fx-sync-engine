import pandas as pd
import great_expectations as gx


def validate_fx_data(message):

    df = pd.DataFrame([message])

    gx_df = gx.from_pandas(df)

    gx_df.expect_column_values_to_not_be_null(
        "exchange_rate"
    )

    gx_df.expect_column_values_to_be_between(
        "exchange_rate",
        min_value=0.0001,
        max_value=1000
    )

    result = gx_df.validate()

    return result["success"]