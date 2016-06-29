import pandas as pd
import numpy as np
import os
import json


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
    """Return a data frame with counts"""
    facility_count_url = base_url + data_tag + "?" + "$select=facility_id,count(*)&$group=facility_id"
    facility_discharge_count_df = pd.read_json(facility_count_url)
    return facility_discharge_count_df


def create_composite_fields_with_id(df, code, description, padding=4, field_suffix="_with_description"):
    zero_padding = "0" * padding
    df[code + "_with_description"] = df.apply(
    lambda x: str(zero_padding + str(int(x[code])))[-padding:] + " - " + str(x[description]), axis=1)
    return df


def prepare_sparcs_df(sparcs_df):
    """Add additional columns to make the SPARCS data easier to work with"""
    sparcs_df["length_of_stay_number"] =  sparcs_df.apply(
        lambda x: 120 if "120 +" == x["length_of_stay"] else int(x["length_of_stay"]), axis=1)

    sparcs_df = create_composite_fields_with_id(sparcs_df, "facility_id", "facility_name")
    sparcs_df = create_composite_fields_with_id(sparcs_df, "apr_drg_code", "apr_drg_description")
    sparcs_df = create_composite_fields_with_id(sparcs_df, "apr_mdc_code", "apr_mdc_description")
    sparcs_df = create_composite_fields_with_id(sparcs_df, "apr_severity_of_illness_code",
                                                "apr_severity_of_illness_description", padding=1)

    sparcs_df["in_hospital_mortality"] = sparcs_df["patient_disposition"] == "Expired"

    return sparcs_df


def generate_facility_df(facility_url):
    facility_df = get_socrata_dataset_in_chunks(facility_url)
    facility_df = create_composite_fields_with_id(facility_df, "apr_drg_code", "apr_drg_description", padding=3)
    facility_df = prepare_sparcs_df(facility_df)

    return facility_df


def get_and_compute_facility_data(facility_id, year, data_tag, data_directory="./", base_url="https://health.data.ny.gov/resource/", refresh=False):
    """Build a table APR"""

    def percentile_25th(x):
        return np.percentile(x, 25)

    def percentile_75th(x):
        return np.percentile(x, 75)

    def percentile_05th(x):
        return np.percentile(x, 5)

    def percentile_95th(x):
        return np.percentile(x, 95)

    def percentile_01th(x):
        return np.percentile(x, 1)

    def percentile_99th(x):
        return np.percentile(x, 99)

    def percentile_50th(x):
        return np.percentile(x, 50)

    facility_id_str = str(int(facility_id))
    facility_url = base_url + data_tag + "?" + "facility_id=" + facility_id_str
    print(facility_url)

    facility_df_csv_name = os.path.join(data_directory, "ny_sparcs_ip_" + facility_id_str + "_" + str(year) + ".csv")

    to_write_csv_flag = 0
    if os.path.exists(facility_df_csv_name):
        if refresh:
            facility_df = generate_facility_df(facility_url)
            to_write_csv_flag = 1
        else:
            facility_df = pd.read_csv(facility_df_csv_name)
    else:
        facility_df = generate_facility_df(facility_url)
        to_write_csv_flag = 1

    if to_write_csv_flag:
        facility_df.to_csv(facility_df_csv_name)

    #print(facility_df[":id"].count())

    apr_drg_stats_df_1 = facility_df.groupby(["apr_drg_code_with_description"]).agg(
        {":id": [np.size], "in_hospital_mortality": [np.sum],
         "length_of_stay_number": [np.mean, np.sum, percentile_50th,
                                   percentile_25th, percentile_75th, percentile_05th, percentile_95th, percentile_01th, percentile_99th],
        }).reset_index()

    apr_drg_stats_df_1[":id"].count()

    apr_drg_stats_df_2 = pd.crosstab(facility_df["apr_drg_code_with_description"], facility_df["age_group"]).reset_index()
    apr_drg_stats_df_3 = pd.merge(apr_drg_stats_df_1, apr_drg_stats_df_2, on="apr_drg_code_with_description", how="outer")
    apr_drg_stats_df_4 = pd.crosstab(facility_df["apr_drg_code_with_description"],
                                 facility_df["apr_severity_of_illness_code_with_description"]).reset_index()
    apr_drg_stats_df_5 = pd.merge(apr_drg_stats_df_3, apr_drg_stats_df_4, on="apr_drg_code_with_description", how="outer")
    apr_drg_stats_df_6 = pd.crosstab(facility_df["apr_drg_code_with_description"], facility_df["apr_risk_of_mortality"]).reset_index()
    apr_drg_stats_df_7 = pd.merge(apr_drg_stats_df_5, apr_drg_stats_df_6, on="apr_drg_code_with_description", how="outer")

    apr_drg_stats_df_8 =  pd.crosstab(facility_df["apr_drg_code_with_description"], facility_df["gender"]).reset_index()
    apr_drg_stats_df_9 = pd.merge(apr_drg_stats_df_7, apr_drg_stats_df_8, on="apr_drg_code_with_description", how="outer")

    apr_drg_stats_df_9.head(10)
    facility_id_with_description = facility_df["facility_id_with_description"].as_matrix()[0]

    discharge_year = facility_df["discharge_year"].as_matrix()[0]

    apr_drg_stats_df_9["discharge_year"] = discharge_year
    apr_drg_stats_df_9["facility_id_with_description"] = facility_id_with_description

    apr_drg_stats_df_10 = pd.crosstab(facility_df["apr_drg_code_with_description"], facility_df["source_of_payment_1"]).reset_index()

    apr_drg_stats_df_11 = pd.merge(apr_drg_stats_df_9, apr_drg_stats_df_10, on="apr_drg_code_with_description", how="outer")

    cleaned_columns = []
    for col in apr_drg_stats_df_11.columns:
        if col.__class__ == (0, 0).__class__:
            cleaned_columns += ["_".join(col)]
        else:
            cleaned_columns += [col]

    cleaned_columns[cleaned_columns.index(":id_size")] = "number_of_discharges"
    cleaned_columns = add_field_suffixes(cleaned_columns)
    apr_drg_stats_df_11.columns = cleaned_columns
    apr_drg_stats_df_11 = apr_drg_stats_df_11.drop("apr_drg_code_with_description_", axis=1)

    apr_drg_stats_df_11["in_hospital_mortality_rate"] = apr_drg_stats_df_11["in_hospital_mortality_sum"] / apr_drg_stats_df_11["number_of_discharges"]
    apr_drg_df_csv_name = os.path.join(data_directory, "sparcs_apr_drg_facility_" + facility_id_str + "_" + str(year) + ".csv")

    apr_drg_stats_df_11.to_csv(apr_drg_df_csv_name)

    return (apr_drg_df_csv_name, facility_df_csv_name)


def main(years_to_process=[2014], data_directory="E:\\data\\sparcs_facility\\"):
    year_to_sparcs_key = {2009: "q6hk-esrj", 2010: "mtfm-rxf4", 2011: "pyhr-5eas", 2012: "u4ud-w55t",
                          2013: "npsr-cm47", 2014: "rmwa-zns4"}

    files_processed = {}
    for year in years_to_process:
        key_year = year_to_sparcs_key[year]

        facility_count_df = get_facility_count_df(data_tag=key_year)
        facility_count_df.sort(columns=["count"], ascending=[0], inplace=True)

        for i in range(len(facility_count_df)):
            try:
                facility_id = int(facility_count_df.iloc[i]["facility_id"])
                facility_apr_drg_csv_name, facility_df_csv_name = get_and_compute_facility_data(facility_id, year, data_tag=key_year, data_directory=data_directory)

                files_processed[str(facility_id) + "_" + str(year)] = {"facility_csv": facility_df_csv_name, "apr_drg_facility_csv": facility_apr_drg_csv_name}
                print(facility_apr_drg_csv_name, facility_df_csv_name)
            except ValueError:
                print(facility_count_df.iloc[i])

    with open(os.path.join(data_directory, "files_processed"), "w") as fw:
        json.dump(files_processed, fw)

    dataframes_concat = []
    # Concatenate into a single data frame
    for key in files_processed:
        dataframes_concat += [pd.read_csv(files_processed[key]["apr_drg_facility_csv"])]

    combined_df = pd.concat(dataframes_concat, axis=0)
    combined_df.to_csv(os.path.join(data_directory, "sparcs_apr_drg_stats.csv"))

def apr_field_suffixes():
    return [
        ('0 - nan', 'APR severity: '),
        ('0 to 17', 'year: '),
        ('1 - Minor', 'APR severity: '),
        ('18 to 29', 'year: '),
        ('2 - Moderate', 'APR severity: '),
        ('3 - Major', 'APR severity: '),
        ('30 to 49', 'year: '),
        ('4 - Extreme', 'APR severity: '),
        ('50 to 69', 'year: '),
        ('70 or Older', 'year: '),
        ('Blue Cross/Blue Shield', 'ins: '),
        ('Department of Corrections', 'ins: '),
        ('Extreme', 'mortality risk: '),
        ('F', 'gender: '),
        ('Federal/State/Local/VA', 'ins: '),
        ('M', 'gender: '),
        ('Major', 'mortality risk: '),
        ('Managed Care, Unspecified', 'ins: '),
        ('Medicaid', 'ins: '),
        ('Medicare', 'ins: '),
        ('Minor', 'mortality risk: '),
        ('Miscellaneous/Other', 'ins: '),
        ('Moderate', 'mortality risk: '),
        ('Private Health Insurance', 'ins: '),
        ('Self-Pay', 'ins: '),
        ('U', 'gender: '),
        ('Unknown', 'mortality risk: '),
        ('Unnamed: 0', 'APR severity: ')
    ]


def add_field_suffixes(columns_to_search):
    field_suffixes_list = apr_field_suffixes()

    for field_suffix in field_suffixes_list:
        if field_suffix[0] in columns_to_search:
            position = columns_to_search.index(field_suffix[0])
            columns_to_search[position] = field_suffix[1] + field_suffix[0]

    return columns_to_search


if __name__ == "__main__":
    main()