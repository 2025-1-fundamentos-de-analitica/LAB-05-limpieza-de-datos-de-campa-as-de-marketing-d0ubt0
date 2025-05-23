# pylint: disable=import-outside-toplevel
import zipfile
import os
import pandas as pd

traductor = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

def dataframe_convertir(df: pd.DataFrame) -> pd.DataFrame:
    df['job'] = df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)

    df['education'] = df['education'].str.replace('.', '_', regex=False)
    df['education'] = df['education'].apply(lambda x: pd.NA if x == 'unknown' else x)

    df['credit_default'] = df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    df['mortgage']       = df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)

    df['previous_outcome'] = df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)

    df['campaign_outcome'] = df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)

    df['last_contact_date'] = ('2022-'+ df['month'].apply(lambda m: f"{traductor[m]:02d}")+ '-'+ df['day'].astype(int).apply(lambda d: f"{d:02d}"))

    return df

def clean_campaign_data():
    clients_df = pd.DataFrame(columns=['client_id', 'age', 'job', 'marital','education', 'credit_default', 'mortgage'])

    campaign_df = pd.DataFrame(columns=['client_id', 'number_contacts', 'contact_duration','previous_campaign_contacts', 'previous_outcome','campaign_outcome', 'last_contact_date'])
    
    economics_df = pd.DataFrame(columns=['client_id', 'cons_price_idx', 'euribor_three_months'])

    for archivo in os.listdir('files/input'):
        ruta_zip = os.path.join('files/input', archivo)
        with zipfile.ZipFile(ruta_zip, 'r') as z:
            nombre_csv = z.namelist()[0]
            with z.open(nombre_csv) as f:
                aux_df = pd.read_csv(f)
                aux_df = dataframe_convertir(aux_df)

                clients_df   = pd.concat([clients_df,   aux_df[clients_df.columns]],   ignore_index=True)
                campaign_df  = pd.concat([campaign_df,  aux_df[campaign_df.columns]],  ignore_index=True)
                economics_df = pd.concat([economics_df, aux_df[economics_df.columns]], ignore_index=True)

    os.makedirs('files/output', exist_ok=True)

    clients_df  .to_csv('files/output/client.csv', index=False)
    campaign_df .to_csv('files/output/campaign.csv', index=False)
    economics_df.to_csv('files/output/economics.csv', index=False)

if __name__ == "__main__":
    clean_campaign_data()
