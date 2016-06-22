import pandas as pd
import numpy as np


def get_socrata_dataset_in_chunks(soda_api_url, limit=10000, order_by=":id"):
    """A simple function to page through results"""
    offset = 0
    data_set_df = None

    while True:
        dataset_url =  soda_api_url +  "&$order=" + order_by + "&$offset=" + str(offset) + "&$limit=" + str(limit) + "&$$exclude_system_fields=false"
        print(dataset_url)
        slice_df = pd.read_json(dataset_url)
        record_count = slice_df[order_by].count()
        if data_set_df is None:
            data_set_df = slice_df
        else:
            data_set_df = data_set_df.append(slice_df)

        if record_count < limit:
            break
        else:
            data_set_df

        offset += limit

    return data_set_df


def get_facility_count_df(base_url="https://health.data.ny.gov/resource/", data_tag="rmwa-zns4"):

    facility_count_url = base_url + data_tag + "?" + "$select=facility_id,count(*)&$group=facility_id"
    facility_discharge_count_df = pd.read_json(facility_count_url)
    return facility_discharge_count_df


def create_composite_fields_with_id(df, code, description, padding=4, field_suffix="_with_description"):
    zero_padding = "0" * padding
    df[code + "_with_description"] = df.apply(
    lambda x: str(zero_padding + str(int(x[code])))[-padding:] + " - " + x[description], axis=1)
    return df

def prepare_sparcs_df(sparcs_df):
    sparcs_df["length_of_stay_number"] =  sparcs_df.apply(
        lambda x: 120 if "120 +" == x["length_of_stay"] else int(x["length_of_stay"]), axis=1)
    sparcs_df = create_composite_fields_with_id(sparcs_df, "facility_id", "facility_name")
    spracs_df = create_composite_fields_with_id(sparcs_df, "apr_drg_code", "apr_drg_description")
    sparcs_df = create_composite_fields_with_id(sparcs_df, "apr_mdc_code", "apr_mdc_description")
    sparcs_df["in_hospital_mortality"] = sparcs_df["patient_disposition"] == "Expired"

    return sparcs_df



def get_facility_data(facility_id, year, data_tag, directory="./", base_url="https://health.data.ny.gov/resource/"):

    def percentile_25th(x):
        return np.percentile(x, 0.25)

    def percentile_75th(x):
        return np.percentile(x, 0.75)

    def percentile_5th(x):
        return np.percentile(x, 0.05)

    def percentile_95th(x):
        return np.percentile(x, 0.95)



    facility_count_url = base_url + data_tag + "?" + "facility_id=" + str(int(facility_id))
    print(facility_count_url)

    facility_df = get_socrata_dataset_in_chunks(facility_count_url)

    print(facility_df[":id"].count())

    facility_df =  create_composite_fields_with_id(facility_df, "apr_drg_code", "apr_drg_description", padding=3)

    facility_df = prepare_sparcs_df(facility_df)


    apr_drg_stats_df_1 = facility_df.groupby(["apr_drg_code_with_description"]).agg(
        {":id": [np.size], "in_hospital_mortality": [np.sum],
         "length_of_stay_number": [np.mean, np.sum, np.median,
                                   percentile_25th, percentile_75th, percentile_5th, percentile_95th],
        }).reset_index()

    apr_drg_stats_df_1[":id"].count()


    apr_drg_stats_df_2 = pd.crosstab(facility_df["apr_drg_code_with_description"], facility_df["age_group"]).reset_index()


    apr_drg_stats_df_3 = pd.merge(apr_drg_stats_df_1, apr_drg_stats_df_2, on="apr_drg_code_with_description", how="outer")


    apr_drg_stats_df_4 = pd.crosstab(facility_df["apr_drg_code_with_description"],
                                 facility_df["apr_severity_of_illness_code"]).reset_index()


    apr_drg_stats_df_5 = pd.merge(apr_drg_stats_df_3, apr_drg_stats_df_4, on="apr_drg_code_with_description", how="outer")


    apr_drg_stats_df_6 = pd.crosstab(facility_df["apr_drg_code_with_description"], facility_df["apr_risk_of_mortality"]).reset_index()


    apr_drg_stats_df_7 = pd.merge(apr_drg_stats_df_5, apr_drg_stats_df_6, on="apr_drg_code_with_description", how="outer")


    apr_drg_stats_df_7.head(5)

    apr_drg_stats_df_8 =  pd.crosstab(facility_df["apr_drg_code_with_description"], facility_df["gender"]).reset_index()

    apr_drg_stats_df_9 = pd.merge(apr_drg_stats_df_7, apr_drg_stats_df_8, on="apr_drg_code_with_description", how="outer")


    # In[39]:

    apr_drg_stats_df_9.head(10)


    # In[40]:

    facility_id_with_description = facility_df["facility_id_with_description"].as_matrix()[0]


    discharge_year = facility_df["discharge_year"].as_matrix()[0]


    apr_drg_stats_df_9["discharge_year"] = discharge_year


    apr_drg_stats_df_9["facility_id_with_description"] = facility_id_with_description

    apr_drg_stats_df_9.head(10)

    apr_drg_stats_df_9.columns

    apr_drg_stats_df_9


    # In[47]:

    # Need to clean the data frame remove replicated columns and rename fields


    # In[48]:

    apr_drg_stats_df_10 = pd.crosstab(facility_df["apr_drg_code_with_description"], facility_df["source_of_payment_1"]).reset_index()


    apr_drg_stats_df_11 = pd.merge(apr_drg_stats_df_9, apr_drg_stats_df_10, on="apr_drg_code_with_description", how="outer")

    apr_drg_stats_df_11

    cleaned_columns = []
    for col in apr_drg_stats_df_11.columns:
        if col.__class__ == (0, 0).__class__:
            cleaned_columns += ["_".join(col)]
        else:
            cleaned_columns += [col]
    apr_drg_stats_df_11.columns = cleaned_columns


