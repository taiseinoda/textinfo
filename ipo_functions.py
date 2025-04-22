import os
import wrds
import pandas as pd
import numpy as np
import getpass
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
from scipy.optimize import minimize
import zipfile
import seaborn as sns
import matplotlib.pyplot as plt
import re
from sklearn.metrics.pairwise import cosine_similarity
from stargazer.stargazer import Stargazer, LineLocation
import statsmodels.formula.api as smf
from rapidfuzz import process, fuzz
import re

def setup(base_path=None,update=True):
    if base_path is None:
        if update == False:
            if os.name == 'posix':
            # MacOS (or Linux)
                base_path = '/Users/taisei/Dropbox/'
            elif os.name == 'nt':
                # Windows
                base_path = 'C:/Users/Taise/Dropbox/'
        else:
            base_path = input("Please enter your dropbox path: (e.g. C:/Users/Taise/Dropbox/)")
    data_path = os.path.join(base_path, "IPOMatch/data/")
    figure_path = os.path.join(base_path, "Apps/Overleaf/IPOMatch/tables_figures/")
    overleaf_path = os.path.join(base_path, "Apps/Overleaf/IPOMatch/tables_figures/")
    print(f"Data path: {data_path}")
    print(f"Figure/Table path: {figure_path}")
    print(f"Overleaf path: {overleaf_path}")
    return data_path,figure_path,overleaf_path

class DataConstructor:
    def __init__(self,data_path):
        self.data_path = data_path
        self.cpi_data = None
        self.wrds_username =input("Enter your WRDS username: ")
        self.wrds_password = getpass.getpass("Enter your WRDS password: ")
        self.conn = None
        self.ccm_lookup = None
        self.issue_date_table = None
    def connect_wrds(self):
        self.conn = wrds.Connection(wrds_username=self.wrds_username,
                                    wrds_password=self.wrds_password)
    def sic_to_fama_french_12(self,sic_code):
        """
        Converts a SIC code to the Fama-French 12 industry categorization.

        Parameters:
        sic_code (int): The SIC code to categorize.

        Returns:
        str: The Fama-French 12 industry category.
        """
        # Define the SIC code ranges for each Fama-French category
        fama_french_12 = {
            'Nodur': [(100, 999), (2000, 2399), (2700, 2749), (2770, 2799), (3100, 3199), (3940, 3989)],
            'Durbl': [(2500, 2519), (2590, 2599), (3630, 3659), (3710, 3711), (3714, 3714), (3716, 3716), (3750, 3751), (3792, 3792), (3900, 3939), (3990, 3999)],
            'Manuf': [(2520, 2589), (2600, 2699), (2750, 2769), (3000, 3099), (3200, 3569), (3580, 3629), (3700, 3709), (3712, 3713), (3715, 3715), (3717, 3749), (3752, 3791), (3793, 3799), (3830, 3839), (3860, 3899)],
            'Enrgy': [(1200, 1399), (2900, 2999)],
            'Chems': [(2800, 2829), (2840, 2899)],
            'BusEq': [(3570, 3579), (3660, 3692), (3694, 3699), (3810, 3829), (7370, 7379)],
            'Telcm': [(4800, 4899)],
            'Utils': [(4900, 4949)],
            'Shops': [(5000, 5999), (7200, 7299), (7600, 7699)],
            'Hlth': [(2830, 2839), (3693, 3693), (3840, 3859), (8000, 8099)],
            'Money': [(6000, 6999)],
            'Other': [(1000, 1199), (1400, 1999), (2400, 2499), (7000, 7199), (7300, 7599), (7700, 7999), (8100, 8999)]
        }

        # Loop through each category and its ranges
        for category, ranges in fama_french_12.items():
            for lower, upper in ranges:
                if lower <= sic_code <= upper:
                    return category

        # If no match, return None or "Other"
        return 'Other'  # Default to "Other"

    def import_sdc(self,search_keyword='SDC Platinum equityissues_taisei'):
        excel_files = [os.path.join(self.data_path+'sdc/', f) for f in os.listdir(self.data_path+'sdc/') if search_keyword in f and f.endswith(('.xls', '.xlsx'))]
        # Initialize an empty list to store DataFrames
        dataframes = []

        # Iterate over each file and read it into a DataFrame
        for file in excel_files:
            try:
                df = pd.read_excel(file,skiprows=2,na_values=['na','NaN'])
                if not df.empty:
                    dataframes.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")

        # Concatenate all DataFrames into one
        if dataframes:
            sdc = pd.concat(dataframes, ignore_index=True)
            print("Concatenated DataFrame is ready for analysis.")
        else:
            print("No matching Excel files found.")
        renaming_columns = {'Issuer/Borrower 6-digit CUSIP':'CUSIP', 'Issuer/Borrower 9-digit CUSIP':'CUSIP9',
            'Issuer/Borrower Ticker Symbol':'ticker',
            'New Issues Fees: Gross Spread per Share or Bond':'gross_spread',
            'New Issues Fees: Gross Spread as Pct of Offer Price':'gross_spread_price',
            'New Issues Fees: Gross Spread as Pct of Principal Amount This Market':'gross_spread_principal',
            'Expenses: Expenses excl Gross Spread as Pct of Total Proceeds':'expenses',
            'Fees: Total Global Gross Spread (USD Millions)':'gross_spread_usd',
            'Principal Amount This Market (USD Millions)':'principal_amount_this_market',
            'Principal Amount All Markets (USD Millions)':'principal_amount_all_market',
            'Primary Shares Offered This Market':'primary_shares_this_market',
            'Primary Shares Filed All Markets':'primary_shares_all_market',
            'Primary Amount Offered This Mkt (USD Millions)':'primary_amount_this_market',
            'Secondary Amount Offered This Mkt (USD Millions)':'secondary_amount_this_market',
            'Full History Total Shares Filed This Market':'full_history',
            'Secondary Shares Offered This Market':'secondary_shares_this_market',
            'Secondary Shares Offered All Markets (No Scaling)': 'secondary_shares_all_market',
            'Proceeds Amount This Market (USD Millions)': 'proceeds_this_market',
            'Proceeds Amount All Markets (USD Millions)':'proceeds_all_market', 'Lead Managers':'lead_managers',
            'Number of Lead, Co-Lead & Co-Managers':'number_managers', 'Issuer/Borrower Zip Code':'zipcode',
            'IPO Flag':'ipo_flag', 'Unit Issues: Unit Issue Flag':'unit_flag',
            'Closed-end Fund/Trust Flag':'closed_flag',
            'Rule 144A Eligible Private Placement Flag (Y/N)':'rule_144A',
            'Private Investment in Public Equities':'pipe', 'Dates: Issue Date':'issue_date',
            'Dates: Filing Date':'filing_date'}
        renaming_columns2 = {'Issuer/Borrower Immediate Parent 6-digit CUSIP':'prt_cusip',
        'Issuer/Borrower Ultimate Parent 6-digit CUSIP':'ult_cusip',
        'Issuer/Borrower Primary SIC':'sic',
        'Issuer/Borrower Ultimate Parent Primary SIC':'mainsic',
        'New Issues Primary SIC (Code)':'mainsiccode', 'New Issues Primary SIC':'sic2',
        'Primary SIC of Exchange Cusip':'sic_ex', 'Postponed Date':'pp_date',
        'Japanese Shelf Filings: Withdrawal Date':'wd_date', 'Date of Lockup Expiration':'lockup_date',
        'Number of Lockup Days':'lockup_days', 'Lockup: Lockup Provision Flag':'lockup', 'Co-Managers':'comanagers',
        'Co-Managers Code':'comng_code', 'Co-Managers Short Name':'comng_short','Issue Type':'issue_type',
        'Offering Technique':'off_tech',
        'Best Efforts/Firm Commitment/Bought Deal Indicator':'commit',
        'All Managers inc Intl Co-Managers Parent Code':'mng_prt_code', 'Master Deal Type':'deal_type',
        'Blank Check (SPAC) Involvement Y/N:':'spac',
        'Venture Capital Backed IPO Issue Flag':'vc',
        'Spinoff (Equity Carveout) Type':'spinoff', 'Issuer/Borrower REIT Type':'reit',
        'New Issues Ratings: Moodys Debt/Bank Loan Rating':'moody_loan_rating'}
        sdc = sdc.rename(columns=renaming_columns)
        sdc = sdc.rename(columns=renaming_columns2)
        sdc_us_common = sdc[(sdc['deal_type']=='US Common Stock')]
        sdc_us_common = sdc_us_common[(sdc_us_common['unit_flag']!=1)&(sdc_us_common['spac']!=1)&(sdc_us_common['reit']!=1)&(sdc_us_common['closed_flag']!=1)].reset_index(drop=True)
        sdc_us_common = sdc_us_common[(sdc_us_common['off_tech'].str.contains('Firm Commitment')==True)].reset_index(drop=True)
        sdc_us_common = sdc_us_common[(sdc_us_common['off_tech'].str.contains('Firm Commitment')==True)].reset_index(drop=True)
        sdc_us_common['mainsiccode'] = pd.to_numeric(sdc_us_common['mainsiccode'], errors='coerce')
        sdc_us_common = sdc_us_common[~sdc_us_common['mainsiccode'].isin(range(6000,7000))].reset_index(drop=True)
        tech_sic_list = [
        3571, 3572, 3575, 3577, 3578,  # computer hardware
        3661, 3663, 3669,              # communications equipment
        3671, 3672, 3674, 3675, 3677, 3678, 3679,  # electronics
        3812,                          # navigation equipment
        3823, 3825, 3826, 3827, 3829,  # measuring and controlling devices
        3841, 3845,                    # medical instruments
        4812, 4813,                    # telephone equipment
        4899,                          # communications services
        7371, 7372, 7373, 7374, 7375, 7378, 7379  # software and related services
        ]
        # https://site.warrington.ufl.edu/ritter/files/IPOs-Tech.pdf
        tech_sic_list2 = [3559,3576,3844,7389]
        sdc_us_common['tech'] = np.where(sdc_us_common['mainsiccode'].isin(tech_sic_list)|sdc_us_common['mainsiccode'].isin(tech_sic_list2),1,0)
        sdc_us_common['ff12'] = sdc_us_common['mainsiccode'].apply(self.sic_to_fama_french_12)
        sdc_us_common['offer_price'] = sdc_us_common['principal_amount_this_market']*1_000_000/(sdc_us_common['primary_shares_this_market']+sdc_us_common['secondary_shares_this_market']) 
        sdc_us_common['issue_date'] = sdc_us_common['issue_date'].dt.floor('D')
        self.issue_date_table = sdc_us_common[['CUSIP','CUSIP9','issue_date','ipo_flag','mainsiccode','prt_cusip','ult_cusip','offer_price']].drop_duplicates().reset_index(drop=True)
        self.issue_date_table.to_pickle(self.data_path+'issue_date_table.pkl')
        sdc_us_common.to_pickle(self.data_path+'sdc_us_common.pkl')
        print('issue_date_table.pkl and sdc_us_common.pkl are saved at', self.data_path)
        return sdc_us_common
    def process_cpi_data(self):
        cpi_df = pd.read_excel(self.data_path + self.cpi_file)
        cpi_long_df = cpi_df.melt(id_vars=['Year'], var_name='Month', value_name='CPI')
        cpi_long_df['Year'] = cpi_long_df['Year'].astype(str)
        cpi_long_df['date'] = pd.to_datetime(cpi_long_df['Year'] + cpi_long_df['Month'], format='%Y%b')
        cpi_long_df['ym'] = cpi_long_df['date'].dt.to_period('M')
        base = cpi_long_df[(cpi_long_df['Year'] == '2010') & (cpi_long_df['Month'] == 'Dec')]['CPI'].iloc[0]
        cpi_long_df['CPI'] = base / cpi_long_df['CPI']
        return cpi_long_df[['ym', 'CPI']]
    
    def get_firm_uw_match(self,sdc_data_input):
        firm_uw_match = sdc_data_input[['CUSIP','CUSIP9','mainsiccode','ff12','prt_cusip','ult_cusip','issue_date','lead_managers','number_managers','comanagers','mng_prt_code','ipo_flag','offer_price','principal_amount_this_market','tech']]
        uw_code = firm_uw_match[['lead_managers', 'number_managers', 'comanagers', 'mng_prt_code']].copy()
        def create_long_format(row):
            lead_managers = str(row['lead_managers']).split(';') if pd.notna(row['lead_managers']) else []
            co_managers = str(row['comanagers']).split(';') if pd.notna(row['comanagers']) else []
            codes = str(row['mng_prt_code']).split('\n') if pd.notna(row['mng_prt_code']) else []

            all_managers = [(manager.strip(), 'lead') for manager in lead_managers] + \
                        [(manager.strip(), 'co') for manager in co_managers]

            rows = []
            for idx, (manager, role) in enumerate(all_managers):
                if idx < len(codes):
                    rows.append({'manager_name': manager, 'role': role, 'mng_prt_code': codes[idx]})
                else:
                    rows.append({'manager_name': manager, 'role': role, 'mng_prt_code': None})
            return rows

        long_format_rows = []
        for _, row in uw_code.iterrows():
            long_format_rows.extend(create_long_format(row))

        long_format_uw_code = pd.DataFrame(long_format_rows)

        # Step 4: Create IPO-firm-level long-format dataframe
        def create_firm_level_long_format(row):
            lead_managers = str(row['lead_managers']).split(';') if pd.notna(row['lead_managers']) else []
            co_managers = str(row['comanagers']).split(';') if pd.notna(row['comanagers']) else []

            all_managers = [(manager.strip(), 'lead') for manager in lead_managers] + \
                        [(manager.strip(), 'co') for manager in co_managers]

            rows = []
            for manager, role in all_managers:
                rows.append({
                    'CUSIP': row['CUSIP'],
                    'CUSIP9': row['CUSIP9'],
                    'ipo_flag': row['ipo_flag'],
                    'prt_cusip': row['prt_cusip'],
                    'ult_cusip': row['ult_cusip'],
                    'mainsiccode': row['mainsiccode'],
                    'tech': row['tech'],
                    'ff12': row['ff12'],
                    'issue_date': row['issue_date'],
                    'offer_price': row['offer_price'],
                    'principal_amount_this_market': row['principal_amount_this_market'],
                    'manager_name': manager,
                    'role': role
                })
            return rows

        firm_level_rows = []
        for _, row in firm_uw_match.iterrows():
            firm_level_rows.extend(create_firm_level_long_format(row))

        all_matches = pd.DataFrame(firm_level_rows)

        # Step 5: Merge the two long-format dataframes
        all_matches = all_matches.merge(
            long_format_uw_code,
            on=['manager_name', 'role'],
            how='left'
        )
        all_matches = all_matches.drop_duplicates().reset_index(drop=True)
        all_matches['issueyear'] = all_matches['issue_date'].dt.year
        all_matches = all_matches.merge(all_matches.groupby(['CUSIP','issue_date'])['manager_name'].nunique().reset_index(name='num_managers'),on=['CUSIP','issue_date'])
        all_matches = all_matches.merge(all_matches.groupby(['CUSIP','issue_date'])['mng_prt_code'].nunique().reset_index(name='num_prt_codes'),on=['CUSIP','issue_date'])
        firm_lead_matches = all_matches[all_matches['role']=='lead'].merge(all_matches[all_matches['role']=='lead'].groupby(['CUSIP','issue_date'])['manager_name'].nunique().reset_index(name='num_leads'),on=['CUSIP','issue_date'])
        uw_code_list = long_format_uw_code[['manager_name','mng_prt_code']].drop_duplicates()
        uw_code_list = uw_code_list.sort_values(by=['mng_prt_code','manager_name']).reset_index(drop=True)
        # Return the two resulting dataframes
        return all_matches,firm_lead_matches,uw_code_list
    
    def get_ccm_lookup(self):
        if self.conn is None:
            self.connect_wrds()
        if self.ccm_lookup is None:
            self.ccm_lookup = self.conn.raw_sql(
                """
                select conm, lpermno as permno, cusip as cusip9, gvkey, cik
                from crsp.ccm_lookup
                """
            )
        ccm_lookup = self.ccm_lookup.rename(columns={'cusip9':'CUSIP9'})
        ccm_lookup['CUSIP'] = ccm_lookup['CUSIP9'].str[:6]
        ccm_lookup = ccm_lookup[['CUSIP','gvkey','permno','cik']].dropna().drop_duplicates().reset_index(drop=True)
        ccm_lookup.to_pickle(self.data_path+'ccm_lookup.pkl')
        return ccm_lookup
    
    def get_compustat_data(self,firm_lead_matches_data):
        firms = firm_lead_matches_data[['CUSIP','mainsiccode','prt_cusip','ult_cusip','issue_date']].drop_duplicates().reset_index(drop=True)
        cusip_list = firms['CUSIP'].dropna().unique().astype(str)
        if self.conn is None:
            self.connect_wrds()
        if self.ccm_lookup is None:
            self.ccm_lookup = self.get_ccm_lookup()
        ccm_lookup = self.ccm_lookup
        firm_cusips = ccm_lookup[ccm_lookup['CUSIP'].isin(cusip_list)]
        gvkey_list = firm_cusips['gvkey'].drop_duplicates().reset_index(drop=True)
        gvkey_list = {'gvkeys':tuple(gvkey_list.astype(str))}
        compq = self.conn.raw_sql(
            """
            select gvkey,datadate, atq,dlcq,revtq,oeps12,epspxq
            from comp.fundq 
            where gvkey in %(gvkeys)s\
            and datadate >= '1975-01-01' 
            and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
            """,
            date_cols=['datadate'],params=gvkey_list
        )
        compa = self.conn.raw_sql(
            """
            select gvkey,datadate,fyr, at,dlc,revt,ebitda,sale,ebit
            from comp.funda 
            where datadate >= '1975-01-01' 
            and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
            and gvkey in %(gvkeys)s\
            order by gvkey, datadate
            """, 
            date_cols=['datadate'],params=gvkey_list
        )
        firms = firms.merge(ccm_lookup[['CUSIP','gvkey']].drop_duplicates(),how='left',on='CUSIP')
        firms = firms.merge(compa,how='left',on=['gvkey'])
        firms['date_diff'] = np.abs(firms['issue_date'] - firms['datadate']).dt.days
        firms = firms.dropna(subset=['CUSIP']).sort_values(by=['CUSIP','date_diff']).drop_duplicates(subset=['CUSIP','issue_date'],keep='first').reset_index(drop=True).drop(columns=['date_diff'])
        firms = firms.merge(compq,how='left',on=['gvkey','datadate'])
        firms['date_diff'] = np.abs(firms['issue_date'] - firms['datadate']).dt.days
        firms = firms.dropna(subset=['CUSIP']).sort_values(by=['CUSIP','date_diff']).drop_duplicates(subset=['CUSIP','issue_date'],keep='first').reset_index(drop=True).drop(columns=['date_diff'])
        return firms
    
    def get_10K_text_data(self,firm_lead_matches_data):
        firms = firm_lead_matches_data[['CUSIP','mainsiccode','prt_cusip','ult_cusip','issue_date']].drop_duplicates().reset_index(drop=True)
        cusip_list = firms['CUSIP'].dropna().unique().astype(str)
        if self.conn is None:
            self.connect_wrds()
        if self.ccm_lookup is None:
            self.ccm_lookup = self.get_ccm_lookup()
        ccm_lookup = self.ccm_lookup
        firm_cusips = ccm_lookup[ccm_lookup['CUSIP'].isin(cusip_list)]
        firm_cusips = firm_cusips.dropna(subset=['cik'])
        firm_cusips['cik'] = pd.to_numeric(firm_cusips['cik'],errors='coerce')
        firm_cusips['cik'] = firm_cusips['cik'].astype(int)
        Form10 = pd.read_csv(self.data_path+'Loughran-McDonald_10X_Summaries_1993-2021.csv')
        Form10['FILING_DATE'] = pd.to_datetime(Form10['FILING_DATE'].astype(str), format='%Y%m%d')
        Form10 = Form10[['FILING_DATE','CIK','N_Words', 'N_Unique_Words', 'N_Negative',
            'N_Positive', 'N_Uncertainty', 'N_Litigious', 'N_StrongModal',
            'N_WeakModal', 'N_Constraining', 'N_Negation']]
        Form10 = Form10.rename(columns={'FILING_DATE':'filing_date','CIK':'cik'})
        for col in Form10[['N_Words', 'N_Unique_Words', 'N_Negative',
            'N_Positive', 'N_Uncertainty', 'N_Litigious', 'N_StrongModal',
            'N_WeakModal', 'N_Constraining', 'N_Negation']]:
            if col != 'N_Words':
                new_col_name = 'Ratio_' + col[2:]  # Remove 'N_' from the column name
                Form10[new_col_name] = Form10[col] / Form10['N_Words']
        Form10 = Form10[['filing_date','cik','N_Words', 'N_Unique_Words', 'N_Negative',
            'N_Positive', 'N_Uncertainty', 'N_Litigious', 'N_StrongModal',
            'N_WeakModal', 'N_Constraining', 'N_Negation','Ratio_Negative',
            'Ratio_Positive', 'Ratio_Uncertainty', 'Ratio_Litigious',
            'Ratio_StrongModal', 'Ratio_WeakModal', 'Ratio_Constraining',
            'Ratio_Negation']]
        Form10 = Form10.merge(firm_cusips,on='cik',how='inner')
        Form10 = Form10[['filing_date','CUSIP','cik','N_Words', 'N_Unique_Words', 'N_Negative',
            'N_Positive', 'N_Uncertainty', 'N_Litigious', 'N_StrongModal',
            'N_WeakModal', 'N_Constraining', 'N_Negation','Ratio_Negative',
            'Ratio_Positive', 'Ratio_Uncertainty', 'Ratio_Litigious',
            'Ratio_StrongModal', 'Ratio_WeakModal', 'Ratio_Constraining',
            'Ratio_Negation']]
        Form10 = Form10.merge(firms[['CUSIP','issue_date']],on='CUSIP',how='inner')
        Form10['date_diff'] = np.abs(Form10['issue_date'] - Form10['filing_date']).dt.days
        Form10 = Form10.dropna(subset=['CUSIP','date_diff']).sort_values(by=['CUSIP','date_diff']).drop_duplicates(subset=['CUSIP','issue_date'],keep='first').reset_index(drop=True)
        return Form10
    
    def get_crsp_data(self,firm_lead_matches_data):
        firms = firm_lead_matches_data[['CUSIP','mainsiccode','prt_cusip','ult_cusip','issue_date']].drop_duplicates().reset_index(drop=True)
        cusip_list = firms['CUSIP'].dropna().unique().astype(str)
        if self.conn is None:
            self.connect_wrds()
        if self.ccm_lookup is None:
            self.ccm_lookup = self.get_ccm_lookup()
        ccm_lookup = self.ccm_lookup
        firm_cusips = ccm_lookup[ccm_lookup['CUSIP'].isin(cusip_list)]
        permno_list = firm_cusips['permno'].dropna().unique().tolist()
        permno_list = {'permnos':tuple(permno_list)}
        dsf_v2 = self.conn.raw_sql(
            """
            select permno,dlycaldt,dlyprc,dlyret,DlyClose,DlyPrcFlg,DlyDelFlg
            from crsp.dsf_v2
            where permno in %(permnos)s
            """,
            date_cols=['dlycaldt'],params=permno_list
        )
        dsi= self.conn.raw_sql(
                """
                select date,vwretx,ewretx
                from crsp.dsi
                """, 
                date_cols=['date']
        )
        dsi = dsi.rename(columns={'date':'dlycaldt'})
        stkdelists = self.conn.raw_sql(
            """
            select permno,delistingdt,delreasontype,delactiontype
            from crsp.stkdelists
            where permno in %(permnos)s\
            """, 
            date_cols=['delistingdt'],params=permno_list
            )
        stkdelists['delisting'] = np.where((~stkdelists['delistingdt'].isna())&((stkdelists['delactiontype']!='MER')&(stkdelists['delactiontype']!='GEX')),1,0)
        crsp_data = dsf_v2.merge(firm_cusips,on='permno',how='inner')
        crsp_data = crsp_data.sort_values(by=['permno','dlycaldt']).drop_duplicates(subset=['permno','dlycaldt'],keep='last')
        crsp_data = crsp_data.merge(stkdelists,on='permno',how='left')
        crsp_data = crsp_data.merge(dsi,on='dlycaldt',how='left')
        return crsp_data

    def compute_performance(self, crsp_data, window_size=30):
        if self.issue_date_table is None:
            self.issue_date_table = pd.read_pickle(self.data_path + 'issue_date_table.pkl')
        if self.ccm_lookup is None:
            self.ccm_lookup = pd.read_pickle(self.data_path + 'ccm_lookup.pkl')

        issue_date_table = self.issue_date_table
        ccm_lookup = self.ccm_lookup

        # Selecting relevant columns and merging with CCM lookup
        issue_date_table = issue_date_table[['CUSIP', 'issue_date', 'ipo_flag', 'offer_price']]
        issue_date_table = issue_date_table.merge(ccm_lookup[['CUSIP', 'permno']].drop_duplicates(),
                                                on='CUSIP', how='inner').dropna()

        # Step 1: Extract IPO dates
        ipo_issue_date_table = issue_date_table[issue_date_table['ipo_flag'] == 1]
        ipo_issue_date_table = (
            ipo_issue_date_table.sort_values(by=['permno', 'issue_date'])
            .drop_duplicates(subset=['permno'], keep='first')
            .rename(columns={'issue_date': 'ipo_issue_date'})
        )

        # Step 2: Extract SEO dates and assign rank
        seo_issue_date_table = issue_date_table[issue_date_table['ipo_flag'] == 0].copy()
        seo_issue_date_table = seo_issue_date_table.sort_values(by=['permno', 'issue_date'])
        seo_issue_date_table['seo_rank'] = seo_issue_date_table.groupby('permno').cumcount() + 1  

        # Step 3: Pivot SEO dates to wide format
        seo_wide = seo_issue_date_table.pivot(index='permno', columns='seo_rank', values='issue_date')
        seo_wide.columns = [f'seo{int(col)}_issue_date' for col in seo_wide.columns]

        # Step 4: Merge IPO and SEO tables
        issue_date_wide = ipo_issue_date_table.merge(seo_wide, on='permno', how='outer')

        # Step 5: Merge with CRSP data
        crsp_data2 = crsp_data[['permno', 'dlycaldt', 'dlyprc', 'dlyret']].reset_index(drop=True)
        crsp_data2 = issue_date_table[['permno', 'CUSIP', 'offer_price']].drop_duplicates(subset=['permno']).reset_index(drop=True).merge(crsp_data2, on='permno', how='inner')
        crsp_data2 = crsp_data2.merge(issue_date_wide.drop(columns=['offer_price']), on='permno', how='inner')

        # Step 6: Compute IPO Initial Returns
        initial_return = crsp_data2[crsp_data2['dlycaldt'] >= crsp_data2['ipo_issue_date']].reset_index(drop=True)
        initial_return['date_diff'] = np.abs((initial_return['dlycaldt'] - initial_return['ipo_issue_date']).dt.days)
        initial_return = initial_return.sort_values(by=['permno', 'date_diff']).drop_duplicates(subset=['permno'], keep='first')
        initial_return['ipo_ir'] = (initial_return['dlyprc'] - initial_return['offer_price']) / initial_return['offer_price']

        # Step 7: Compute SEO Initial Returns (seo_ir)
        seo_ir_df = pd.DataFrame()
        for rank in range(1, 4):  # Assuming max 3 SEOs
            seo_var = f'seo{rank}_issue_date'
            seo_ir_temp = crsp_data2.dropna(subset=['dlyprc', seo_var]).reset_index(drop=True)
            seo_ir_temp['date_diff'] = np.abs((seo_ir_temp['dlycaldt'] - seo_ir_temp[seo_var]).dt.days)
            seo_ir_temp = seo_ir_temp.sort_values(by=['permno', 'date_diff']).drop_duplicates(subset=['permno'], keep='first')
            seo_ir_temp[f'seo{rank}_ir'] = (seo_ir_temp['dlyprc'] - seo_ir_temp['offer_price']) / seo_ir_temp['offer_price']
            seo_ir_df = seo_ir_df.merge(seo_ir_temp[['permno', f'seo{rank}_ir']], on='permno', how='outer') if not seo_ir_df.empty else seo_ir_temp[['permno', f'seo{rank}_ir']]

        # Step 8: Compute Performance Metrics (ex-post & ex-ante)
        stock_perf = pd.DataFrame()
        for variable in ['ipo_issue_date', 'seo1_issue_date']:
            perf = crsp_data2.dropna(subset=['dlyret', variable]).reset_index(drop=True)
            perf['date_diff'] = np.abs((perf['dlycaldt'] - perf[variable]).dt.days)

            # Ex-post performance
            ex_post_perf = perf[(perf['date_diff'] <= window_size) & (perf['dlycaldt'] >= perf[variable])]
            ex_post_perf = ex_post_perf.groupby('permno')['dlyret'].agg(['mean', 'std']).rename(columns={'mean': 'mean', 'std': 'vol'})
            ex_post_perf['sr'] = ex_post_perf['mean'] / ex_post_perf['vol']

            ex_post_perf = ex_post_perf.rename(columns={'mean': f'{variable[:-11]}_mean', 
                                                        'vol': f'{variable[:-11]}_vol', 
                                                        'sr': f'{variable[:-11]}_sr'})

            if variable == 'seo1_issue_date':  # Compute ex-ante only for SEO
                ex_ante_perf = perf[(perf['date_diff'] <= window_size) & (perf['dlycaldt'] < perf[variable])]
                ex_ante_perf = ex_ante_perf.groupby('permno')['dlyret'].agg(['mean', 'std']).rename(columns={'mean': 'mean', 'std': 'vol'})
                ex_ante_perf['sr'] = ex_ante_perf['mean'] / ex_ante_perf['vol']

                ex_ante_perf = ex_ante_perf.rename(columns={'mean': 'seo1_exante_mean', 
                                                            'vol': 'seo1_exante_vol', 
                                                            'sr': 'seo1_exante_sr'})

                ex_post_perf = ex_post_perf.merge(ex_ante_perf, on='permno', how='outer')

            stock_perf = stock_perf.merge(ex_post_perf, on='permno', how='outer') if not stock_perf.empty else ex_post_perf

        # Step 9: Merge all results
        perf = issue_date_wide.merge(initial_return[['permno', 'ipo_ir']], on='permno', how='left')
        perf = perf.merge(seo_ir_df, on='permno', how='left')  # Merge SEO IRs
        perf = perf.merge(stock_perf, on='permno', how='left')
        delist_info = crsp_data[['permno','delistingdt','delreasontype','delactiontype','delisting']].dropna(subset=['permno','delistingdt']).drop_duplicates(subset=['permno','delistingdt'],keep='last')
        perf = perf.merge(delist_info, on='permno', how='left')
        perf['delist_age'] = (perf['delistingdt']-perf['ipo_issue_date']).dt.days
        perf['delist_age'] = np.where(perf['delist_age']<0, np.nan, perf['delist_age'])
        return perf
    
    def get_ipo_data(self):
        if self.issue_date_table is None:
            self.issue_date_table = pd.read_pickle(self.data_path+'issue_date_table.pkl')
        issue_date_table = self.issue_date_table
        ipo_firms = issue_date_table[(issue_date_table['ipo_flag'] == 1)]
        ipoage = (pd.read_excel(self.data_path +"IPO-age.xlsx",na_values=['.','-9','-99'], dtype={'CUSIP': str})
        .iloc[:,:-3]
        .assign(CUSIP8=lambda d: d['CUSIP'])
        .assign(CUSIP=lambda d:d['CUSIP'].str[:6])
        .rename(columns={'Offer date':'issuedate','IPO name':'IPOname','CRSP permanent ID':'permno'})
        .assign(issuedate=lambda d:pd.to_datetime(d['issuedate'],format='%Y%m%d'))
        .assign(issueyear=lambda d:d['issuedate'].dt.year)
        .assign(firmage=lambda d:d['issueyear']-d['Founding'])
        .assign(VCinGC=lambda d:(d['VC']==1)|(d['VC']==2))
        .assign(VCinGC=lambda d:d['VCinGC']*1)
        .drop(columns='VC')
        .dropna(subset=['permno','CUSIP'])
        .reset_index(drop=True)
        )
        ipoage['issueyear'] = ipoage['issueyear'].astype(int)
        ipoage['firmage'] = np.where(ipoage['firmage']<0, np.nan, ipoage['firmage'])
        ipo_firms= pd.merge(ipo_firms,ipoage,how='inner',on='CUSIP')
        jr_analyst = pd.read_excel(self.data_path+'IPO-Analyst-Data-Online-1993-2009-2011-04-01.xls',sheet_name='DATA')
        jr_analyst = jr_analyst.rename(columns={'PERM':'permno'})
        jr_analyst['ipodate'] = pd.to_datetime(jr_analyst['ipodate'], format='%Y%m%d')
        jr_analyst['as_analyst'] = jr_analyst['ANALYST1'].isin(range(1,4)).astype(int)
        jr_analyst['asru_analyst'] = (jr_analyst['ANALYST1']>0).astype(int)
        jr_analyst['analyst'] =  jr_analyst['ANALYST1'].isin(range(1,5)).astype(int)
        jr_analyst = jr_analyst.drop(columns=['ANALYST1','ANALYST2','ANALYST3','ANALYST4','UWCODE01','UWCODE02','UWCODE03','UWCODE04','UWROLE01','UWROLE02','UWROLE03','UWROLE04'])
        ipo_firms = ipo_firms.merge(jr_analyst[['permno','as_analyst','asru_analyst','analyst']],on=['permno'],how='left')
        return ipo_firms
    
    def merge_firm_data(self,compustat_data,text_data,crsp_performance,ipo_firms):
        firm_data = compustat_data.merge(text_data,on=['CUSIP','issue_date'],how='left',indicator=True).rename(columns={'_merge':'compustat_text_merge'})
        firm_data = firm_data.merge(crsp_performance[['permno','CUSIP','ipo_issue_date','ipo_ir','ipo_mean','ipo_vol','ipo_sr']].dropna(subset=['permno','ipo_issue_date']),left_on=['CUSIP','issue_date'],right_on=['CUSIP','ipo_issue_date'],how='left',indicator=True).rename(columns={'_merge':'ipo_crsp_merge'})
        firm_data = firm_data.merge(crsp_performance[['CUSIP','seo1_issue_date','seo1_ir','seo1_exante_mean','seo1_exante_vol','seo1_exante_sr','seo1_mean','seo1_vol','seo1_sr']].dropna(subset=['CUSIP','seo1_issue_date']),left_on=['CUSIP','issue_date'],right_on=['CUSIP','seo1_issue_date'],how='left',indicator=True).rename(columns={'_merge':'seo1_crsp_merge'})
        firm_data = firm_data.merge(ipo_firms.drop(columns= ['mainsiccode', 'prt_cusip', 'ult_cusip']),on=['CUSIP','permno','issue_date'],how='left',indicator=True).rename(columns={'_merge':'ipo_firms_merge'})
        firm_data['ipo_flag'] = np.where(firm_data['ipo_flag']==1,1,0).astype(int)
        firm_data['issueyear'] = firm_data['issue_date'].dt.year
        return firm_data
    
    def pull_firm_data_from_wrds(self,ipo_info_input,skip_sql=True):
        ipo_firms = ipo_info_input[['IPOname','CUSIP','mainsiccode','prt_cusip','ult_cusip','issuedate','permno','offer_price','firmage','VCinGC','gross_spread_usd','proceeds_all_market']].drop_duplicates()
        permnolist  = {'permnos':tuple(ipo_firms['permno'].unique().tolist())}
        if self.conn is None:
            self.connect_wrds()
        stkdelists = self.conn.raw_sql(
                """
                select permno,delistingdt,delreasontype,delactiontype
                from crsp.stkdelists
                where permno in %(permnos)s\
                """, 
                date_cols=['delistingdt'],params=permnolist
            )
        stkdelists.to_pickle(self.data_path+'stkdelists.pkl')
        print('stkdelists.pkl is saved at',self.data_path+'stkdelists.pkl')
        dsf_v2 = self.conn.raw_sql(
                """
            select permno,dlycaldt,dlyprc,dlyret,DlyClose,DlyPrcFlg,DlyDelFlg
            from crsp.dsf_v2
            where permno in %(permnos)s\
            """, 
            date_cols=['dlycaldt'],params=permnolist
            )
        dsf_v2['dlycaldt'] = pd.to_datetime(dsf_v2['dlycaldt'])
        dsi= self.conn.raw_sql(
                """
                select date,vwretx,ewretx
                from crsp.dsi
                """, 
                date_cols=['date']
        )
        dsi = dsi.rename(columns={'date':'dlycaldt'})
        dsf_v2 = dsf_v2.merge(dsi, on='dlycaldt', how='inner')
        dsf_v2.to_pickle(self.data_path+'dsf_v2.pkl')
        link = self.conn.raw_sql(
            """
            select distinct gvkey, lpermno as permno, linkdt, linkenddt
            from crsp.Ccmxpf_linktable
            where linktype in ('LU', 'LC')
            and LINKPRIM in ('P', 'C')
            and lpermno in %(permnos)s\
            """,
            params=permnolist
        )
        link['gvkey'] = link['gvkey'].astype(str)
        link['permno'] = link['permno'].astype(int)
        link['linkenddt'] = link['linkenddt'].fillna(pd.Timestamp('21000101'))
        gvkey_list = link['gvkey'].drop_duplicates().reset_index(drop=True)
        gvkey_list = {'gvkeys':tuple(gvkey_list.astype(str))}
        compq = self.conn.raw_sql(
            """
            select gvkey,datadate, atq,dlcq,revtq,oeps12,epspxq,fyr
            from comp.fundq 
            where datadate >= '1975-01-01' 
            and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
            and gvkey in %(gvkeys)s\
            order by gvkey, datadate
            """, 
            date_cols=['datadate'],params=gvkey_list
        )
        compa = self.conn.raw_sql(
            """
            select gvkey,datadate,fyr, at,dlc,revt,ebitda,sale,ebit
            from comp.funda 
            where datadate >= '1975-01-01' 
            and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
            and gvkey in %(gvkeys)s\
            order by gvkey, datadate
            """, 
            date_cols=['datadate'],params=gvkey_list
        )
        compa = compa.merge(link, on='gvkey', how='inner')
        compa = compa[(compa['linkdt']<=compa['datadate'])&(compa['datadate']<=compa['linkenddt'])]
        compa = compa.sort_values(by=['permno','datadate','linkdt']).drop_duplicates(subset=['permno','datadate'],keep='last')
        compa.to_pickle(self.data_path+'compa.pkl')
        print('compa.pkl is saved at',self.data_path+'compa.pkl')
        compq = compq.merge(link, on='gvkey', how='inner')
        compq = compq[(compq['linkdt']<=compq['datadate'])&(compq['datadate']<=compq['linkenddt'])]
        compq = compq.sort_values(by=['permno','datadate','linkdt']).drop_duplicates(subset=['permno','datadate'],keep='last')
        compq.to_pickle(self.data_path+'compq.pkl')
        print('compq.pkl is saved at',self.data_path+'compq.pkl')  
        return dsf_v2,compa,compq,stkdelists
    
    def construct_ipo_firm_data(self,ipo_info_input,dsf_v2,compa,stkdelists):
        initial_returns = ipo_info_input[['permno','issuedate','offer_price','ff12','tech','CUSIP','CUSIP8','prt_cusip','ult_cusip']].dropna().drop_duplicates().merge(dsf_v2,on='permno',how='inner')
        initial_returns['issuedate_dlycaldt'] = (initial_returns['issuedate']-initial_returns['dlycaldt']).dt.days
        initial_returns = initial_returns[initial_returns['issuedate_dlycaldt']>=0]
        initial_returns = initial_returns.sort_values(by=['permno','issuedate_dlycaldt']).drop_duplicates(subset=['permno'],keep='first')
        initial_returns['initial_return'] = (initial_returns['dlyprc']-initial_returns['offer_price'])/initial_returns['offer_price']
        initial_returns['ym'] = initial_returns['issuedate'].dt.to_period('M')
        initial_returns = initial_returns.merge(initial_returns.groupby(['ym'])['initial_return'].mean().reset_index(name='ir_mth_avg'),on=['ym'],how='inner')
        initial_returns['ir_from_mean'] = (initial_returns['initial_return']-initial_returns['ir_mth_avg'])
        initial_returns['ir_mkt'] = initial_returns['initial_return'] - initial_returns['ewretx']
        volatility = ipo_info_input[['permno','issuedate','offer_price']].dropna().drop_duplicates().merge(dsf_v2,on='permno',how='inner')
        volatility = volatility[(volatility['dlycaldt']>=volatility['issuedate'])&(volatility['dlycaldt']<=volatility['issuedate']+pd.Timedelta(days=30))]
        volatility['dlyret'] = np.where(volatility['dlycaldt']==volatility['issuedate'],(volatility['dlyprc']-volatility['offer_price'])/volatility['offer_price'],volatility['dlyret'])
        volatility = volatility.groupby(['permno','issuedate'])['dlyret'].agg(['mean','std']).reset_index()
        volatility.columns = ['permno','issuedate','ir_30day_avg','ir_30day_vol']
        ipo_returns = volatility.merge(initial_returns,on=['permno','issuedate'],how='inner')
        ipo_returns['ir_30day_sr'] = ipo_returns['ir_30day_avg']/ipo_returns['ir_30day_vol']
        ipo_returns = ipo_returns.merge(stkdelists,on='permno',how='left')
        ipo_returns['delisting'] = np.where((~ipo_returns['delistingdt'].isna())&((ipo_returns['delactiontype']!='MER')&(ipo_returns['delactiontype']!='GEX')),1,0)
        ipo_returns['life'] = (ipo_returns['delistingdt']-ipo_returns['issuedate']).dt.days
        comp_info = ipo_info_input[['permno','issuedate']].dropna().drop_duplicates()
        comp_info = comp_info.merge(compa.drop(columns=['gvkey','fyr','linkdt','linkenddt']),on=['permno'],how='inner')
        comp_info['compustat_date_issuedate'] = np.abs((comp_info['datadate']-comp_info['issuedate']).dt.days)
        comp_info = comp_info.sort_values(by=['permno','compustat_date_issuedate']).drop_duplicates(subset=['permno'],keep='first')
        ipo_firms = ipo_returns.merge(comp_info,on=['permno','issuedate'],how='inner')
        cik_cusip_map = pd.read_csv(self.data_path+'cik-cusip-mapping-master/cik-cusip-maps.csv')
        cik_cusip_map = cik_cusip_map.rename(columns={'cusip6':'CUSIP','cusip8':'CUSIP8','cik':'CIK'})
        ipo_firms = ipo_firms.drop(columns=['CUSIP8']).merge(cik_cusip_map,on='CUSIP',how='left')
        return ipo_firms

    def get_additional_ipo_firm_data(self,ipo_firms_input):
        jr_analyst = pd.read_excel(self.data_path+'IPO-Analyst-Data-Online-1993-2009-2011-04-01.xls',sheet_name='DATA')
        jr_analyst = jr_analyst.rename(columns={'PERM':'permno'})
        jr_analyst['ipodate'] = pd.to_datetime(jr_analyst['ipodate'], format='%Y%m%d')
        jr_analyst['as_analyst'] = jr_analyst['ANALYST1'].isin(range(1,4)).astype(int)
        jr_analyst['asru_analyst'] = (jr_analyst['ANALYST1']>0).astype(int)
        jr_analyst['analyst'] =  jr_analyst['ANALYST1'].isin(range(1,5)).astype(int)
        jr_analyst = jr_analyst.drop(columns=['ANALYST1','ANALYST2','ANALYST3','ANALYST4','UWCODE01','UWCODE02','UWCODE03','UWCODE04','UWROLE01','UWROLE02','UWROLE03','UWROLE04'])
        ipo_firms_output = ipo_firms_input.merge(jr_analyst[['permno','as_analyst','asru_analyst','analyst']],on=['permno'],how='left')
        Form10 = pd.read_csv(self.data_path+'Loughran-McDonald_10X_Summaries_1993-2021.csv')
        Form10['FILING_DATE'] = pd.to_datetime(Form10['FILING_DATE'].astype(str), format='%Y%m%d')
        Form10 = Form10[['FILING_DATE','CIK','N_Words', 'N_Unique_Words', 'N_Negative',
            'N_Positive', 'N_Uncertainty', 'N_Litigious', 'N_StrongModal',
            'N_WeakModal', 'N_Constraining', 'N_Negation']]
        Form10 = Form10.rename(columns={'FILING_DATE':'filing_date'})
        for col in Form10[['N_Words', 'N_Unique_Words', 'N_Negative',
            'N_Positive', 'N_Uncertainty', 'N_Litigious', 'N_StrongModal',
            'N_WeakModal', 'N_Constraining', 'N_Negation']]:
            if col != 'N_Words':
                # Compute the ratio and create a new column 'Ratio_x'
                new_col_name = 'Ratio_' + col[2:]  # Remove 'N_' from the column name
                Form10[new_col_name] = Form10[col] / Form10['N_Words']
        Form10 = Form10[['filing_date','CIK','Ratio_Negative',
            'Ratio_Positive', 'Ratio_Uncertainty', 'Ratio_Litigious',
            'Ratio_StrongModal', 'Ratio_WeakModal', 'Ratio_Constraining',
            'Ratio_Negation']]
        ipo_firms_output = ipo_firms_output.merge(Form10,on=['CIK'],how='left')
        ipo_firms_output['filing_date_issuedate'] = np.abs((ipo_firms_output['filing_date']-ipo_firms_output['issuedate']).dt.days)
        ipo_firms_output = ipo_firms_output.sort_values(by=['permno','filing_date_issuedate']).drop_duplicates(subset=['permno'],keep='first')
        return ipo_firms_output
    
    
    def construct_uw_rank(self,all_matches_input,uw_code_list_input,drop_na_rank=True):
        uw_list = uw_code_list_input.copy()
        def clean_name(name):
            common_terms = ["&", "co.", "inc.", "ltd.", "corp.", "corporation", "company", "inc", "ltd", "corp","llc","incorporated"]
            # Remove common terms
            name = re.sub(r'\b(?:' + '|'.join(map(re.escape, common_terms)) + r')\b', '', name, flags=re.IGNORECASE)
            # Replace "-" with a space and remove other punctuation
            name = re.sub(r'-', ' ', name)
            name = re.sub(r'[&.,]', '', name)
            # Remove extra spaces
            name = re.sub(r'\s+', ' ', name).strip()
            return name
        uw_list['manager_name_cleaned'] = uw_list['manager_name'].str.lower().str.strip().apply(clean_name)
        uw_rank = pd.read_excel(self.data_path+'Underwriter-Rank.xls')
        uw_rank = uw_rank.drop(columns=['Unnamed: 13']).dropna(subset=['Underwriter Name']).drop_duplicates(subset=['Underwriter Name'])
        uw_rank = uw_rank[~uw_rank['Underwriter Name'].str.contains('no activity')]
        uw_rank_long = pd.wide_to_long(
            uw_rank,
            stubnames='Rank',  # Common prefix in column names
            i='Underwriter Name',  # Identifier column
            j='Year',  # Year suffix
            sep='',  # No separator between 'Rank' and year in column names
            suffix=r'\d+'
        ).reset_index()

        year_mapping = {
            8084: range(1980, 1985),
            8591: range(1985, 1992),
            9200: range(1992, 2001),
            104: range(2001, 2005),
            507: range(2005, 2008),
            809: range(2008, 2010),
            1011: range(2010, 2012),
            1217: range(2012, 2018),
            1820: range(2018, 2021),
            2122: range(2021, 2023),
            23: range(2023, 2024),
            24: range(2024, 2025),
        }

        # Create a dataframe with expanded years
        uw_rank_long = (
            uw_rank_long.assign(Year=uw_rank_long['Year'].map(year_mapping))  # Replace suffix with range
            .explode('Year')  # Expand rows for each year in the range
            .reset_index(drop=True)
        )
        uw_rank_long['Rank'] = pd.to_numeric(uw_rank_long['Rank'], errors='coerce')
        uw_rank_long['Rank'] = np.round(uw_rank_long['Rank'],0)
        def fuzzy_match(df1, df2, column1, column2, threshold=80):
            matches = []
            for name in df1[column1]:
                match = process.extractOne(name, df2[column2], scorer=fuzz.token_sort_ratio)
                if match and match[1] >= threshold:
                    matches.append((name, match[0], match[1]))
            return pd.DataFrame(matches, columns=[column1, column2, 'Similarity'])

        # Updated cleaning function to replace "-" with a space
        
        uw_rank_name_list = uw_rank_long[['Underwriter Name','Rank']].drop_duplicates(subset=['Underwriter Name'])
        uw_rank_name_list['Underwriter Name Cleaned'] = uw_rank_name_list['Underwriter Name'].str.lower().str.strip().apply(clean_name)
        matched_names = fuzzy_match(uw_rank_name_list,  uw_list, 'Underwriter Name Cleaned', 'manager_name_cleaned', threshold=80)
        uw_list = uw_list.merge(matched_names, left_on='manager_name_cleaned', right_on='manager_name_cleaned', how='left')
        uw_list = uw_list.merge(uw_rank_name_list[['Underwriter Name','Underwriter Name Cleaned']],on='Underwriter Name Cleaned',how='left')
        manual_match_table = pd.read_excel(self.data_path+'uw_code_rank_manual_match.xlsx')
        manual_match_table['revised'] = 1.0
        uw_list = uw_list.merge(manual_match_table[['manager_name','Underwriter Name','revised']],left_on='manager_name',right_on='manager_name',how='left')
        uw_list['Underwriter Name'] = uw_list['Underwriter Name_y'].fillna(uw_list['Underwriter Name_x'])
        uw_list['Underwriter Name'] = np.where(uw_list['revised']==1.0,uw_list['Underwriter Name_y'],uw_list['Underwriter Name'])
        uw_list['revised'] = np.where(uw_list['revised']==1.0,1.0,0.0)
        uw_list = uw_list.sort_values(by=['manager_name','revised']).drop_duplicates(subset=['manager_name'],keep='last').reset_index(drop=True)
        uw_list = uw_list[['manager_name','mng_prt_code','Underwriter Name']]
        uw_active = all_matches_input.copy()
        uw_rank_year = uw_active[['manager_name','mng_prt_code','issueyear']].drop_duplicates(subset=['manager_name','issueyear'])
        uw_rank_year = uw_rank_year.drop(columns=['mng_prt_code']).merge(uw_list,on='manager_name',how='left')
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Barclays'))&(uw_rank_year['issueyear']>=1992)&(uw_rank_year['issueyear']<=2000), 'Barclay Investments, Inc.', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Barclays'))&(uw_rank_year['issueyear']>=2005), 'Barclays Capital', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Banco BTG Pactual SA'), 'BTG Pactual', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Cantor Fitzgerald Inc'), 'Cantor, Fitzgerald & Co., Inc.', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='China International Capital Corp HK Securities Ltd'), 'China International Capital Corp (CICC)', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Citi'), 'Citigroup', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('China Renaissance'))&(uw_rank_year['issueyear']>=2012), 'China Renaissance (CRS)', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Berenberg Capital Markets LLC'), 'Berenberg', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Mizuho Securities')), 'Mizuho Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Nomura Securities International Inc'), 'Nomura Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Cowen Securities LLC'), 'Cowen', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Credit Suisse'))&(uw_rank_year['issueyear']>=1992)&(uw_rank_year['issueyear']<=2000), 'Credit Suisse First Boston', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Credit Suisse'))&(uw_rank_year['issueyear']>=1992)&(uw_rank_year['issueyear']<=2011), 'Credit Suisse First Boston', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Credit Suisse'))&(uw_rank_year['issueyear']>=2012), 'Credit Suisse', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('UBS'))&(uw_rank_year['issueyear']>=2001), 'UBS Investment Bank', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('UBS'))&(uw_rank_year['issueyear']>=1992)&(uw_rank_year['issueyear']<=2000), 'UBS Ltd', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='ViewTrade Securities Inc'), 'VIEWT-SEC', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Wedbush Securities, Inc.')&(uw_rank_year['issueyear']>=2005), 'Wedbush Morgan Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Merrill Lynch Pierce Fenner & Smith')&(uw_rank_year['issueyear']>=2005), 'Merrill Lynch & Co Inc', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='BofA Securities Inc'), 'Banc of America Securities LLC', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='BA Securities Inc')&(uw_rank_year['issueyear']>=2001), 'Banc of America Securities LLC', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Boustead Securities LLC'), 'Boustead Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='GKN Securities Corp'), 'GKN Securities Corp', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('JP Morgan'))&(uw_rank_year['issueyear']>=2005), 'JP Morgan (JPM)', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Northland Securities Inc'), 'Northland-Sec', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Piper Jaffray Cos')&(uw_rank_year['issueyear']>=2005), 'Piper Jaffray Inc', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Santander Investment Securities Inc'), 'Santander Investment Bank', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Sentra Securities Corp'), 'Sentra Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('HSBC'))&(uw_rank_year['issueyear']>=2010), 'HSBC', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Leerink'))&(uw_rank_year['issueyear']<=2020), 'Leerink Swann & Co.', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Robinson-Humphrey Co')&(uw_rank_year['issueyear']<=2000), 'Robinson-Humphrey (Old)', uw_rank_year['Underwriter Name'])
        uw_rank_year = uw_rank_year.merge(uw_rank_long.rename(columns={'Rank':'rank'}),left_on=['Underwriter Name','issueyear'],right_on=['Underwriter Name','Year'],how='left')
        uw_rank_year['rank'] = np.where(uw_rank_year['rank']<0, np.nan, uw_rank_year['rank'])
        if drop_na_rank:
            uw_rank_year = uw_rank_year.dropna(subset=['rank']).reset_index(drop=True)
        return uw_rank_year
    
    def compute_uw_share(self,all_matches):
        num_managers = all_matches.groupby(['CUSIP','issue_date','role'])['mng_prt_code'].nunique().reset_index()
        num_managers = num_managers.pivot_table(index=['CUSIP','issue_date'],columns='role',values='mng_prt_code').reset_index()
        num_managers.columns = ['CUSIP','issue_date','co_num_managers','lead_num_managers']
        num_managers = num_managers.fillna(0)
        all_matches2 = all_matches.merge(num_managers,on=['CUSIP','issue_date'],how='inner')
        all_matches2['principal_amount_per_lead'] = np.where(all_matches2['lead_num_managers'] > 0,all_matches2['principal_amount_this_market']/all_matches2['lead_num_managers'],0)
        all_matches2['principal_amount_per_co'] = np.where(all_matches2['co_num_managers'] > 0,all_matches2['principal_amount_this_market']/all_matches2['co_num_managers'],0)
        principal_amount_per_uw = all_matches2[['CUSIP','role','ipo_flag','issueyear','mng_prt_code','principal_amount_per_lead','principal_amount_per_co']].copy()
        principal_amount_per_uw['principal_amount_per_uw'] = np.where(principal_amount_per_uw['role'] == 'lead',principal_amount_per_uw['principal_amount_per_lead'],principal_amount_per_uw['principal_amount_per_co'])
        principal_amount_per_uw = principal_amount_per_uw.drop(columns=['principal_amount_per_lead','principal_amount_per_co'])

        uwshare = principal_amount_per_uw.groupby(['mng_prt_code','issueyear','role','ipo_flag'])['principal_amount_per_uw'].sum().reset_index()
        uwshare = uwshare.pivot_table(index=['mng_prt_code','issueyear'],columns=['role','ipo_flag'],values='principal_amount_per_uw').reset_index()
        uwshare.columns = ['mng_prt_code','issueyear','co_seo_amount','co_ipo_amount','lead_seo_amount','lead_ipo_amount']
        uwshare = uwshare.fillna(0)
        uwshare['co_total_amount'] = uwshare['co_seo_amount'] + uwshare['co_ipo_amount']
        uwshare['lead_total_amount'] = uwshare['lead_seo_amount'] + uwshare['lead_ipo_amount']
        uwshare['ipo_total_amount'] = uwshare['co_ipo_amount'] + uwshare['lead_ipo_amount']
        uwshare['seo_total_amount'] = uwshare['co_seo_amount'] + uwshare['lead_seo_amount']
        variables = ['co_total_amount','lead_total_amount','ipo_total_amount','seo_total_amount','co_seo_amount','lead_seo_amount','co_ipo_amount','lead_ipo_amount']
        uwshare_sum = uwshare.groupby(['issueyear'])[variables].sum().reset_index()
        uwshare = uwshare.merge(uwshare_sum,on='issueyear',how='left',suffixes=('','_sum'))
        for variable in variables:
            uwshare[variable+'_share'] = uwshare[variable]/uwshare[variable+'_sum']
        return uwshare


    def underwriter_performance(self,crsp_perf_data,all_matches):
        ipo_ir_data = crsp_perf_data[['CUSIP','ipo_issue_date','ipo_ir','ipo_vol','ipo_sr']].copy()
        ipo_ir_data['ym'] = ipo_ir_data['ipo_issue_date'].dt.to_period('M')
        ir_monthly_avg = ipo_ir_data.groupby(['ym'])[['ipo_ir','ipo_vol','ipo_sr']].mean().reset_index().rename(columns={'ipo_ir':'ipo_ir_monthly_avg','ipo_vol':'ipo_vol_monthly_avg','ipo_sr':'ipo_sr_monthly_avg'})
        ipo_ir_data = ipo_ir_data.merge(ir_monthly_avg,on='ym',how='left')
        ipo_ir_data['ipo_ir_premium'] = ipo_ir_data['ipo_ir'] - ipo_ir_data['ipo_ir_monthly_avg']
        ipo_ir_data['ipo_vol_premium'] = ipo_ir_data['ipo_vol'] - ipo_ir_data['ipo_vol_monthly_avg']
        ipo_ir_data['ipo_sr_premium'] = ipo_ir_data['ipo_sr'] - ipo_ir_data['ipo_sr_monthly_avg']
        ipo_ir_data = ipo_ir_data.drop(columns=['ym','ipo_ir_monthly_avg','ipo_vol_monthly_avg','ipo_sr_monthly_avg'])
        lead_matches = all_matches[all_matches['role'] == 'lead'].copy()
        ipo_lead_matches = lead_matches[lead_matches['ipo_flag'] == 1].copy()
        ipo_lead_matches = ipo_lead_matches[['CUSIP','mng_prt_code','issue_date']].merge(ipo_ir_data,left_on=['CUSIP','issue_date'],right_on=['CUSIP','ipo_issue_date'],how='inner')
        def compute_uw_past5y_avg(df, value_var):
            df = df.copy()
            
            # Ensure date is datetime
            df['issue_date'] = pd.to_datetime(df['issue_date'])
            
            # Sort for stability
            df = df.sort_values(by=['mng_prt_code', 'issue_date']).reset_index(drop=True)
            
            # Prepare output column
            avg_col = 'uw_' + value_var
            df[avg_col] = np.nan

            # Loop through rows
            for idx, row in df.iterrows():
                uw = row['mng_prt_code']
                date = row['issue_date']
                start_date = date - pd.DateOffset(years=5)
                
                # Filter: same UW, in window, and exclude self
                mask = (
                    (df['mng_prt_code'] == uw) &
                    (df['issue_date'] >= start_date) &
                    (df['issue_date'] < date) &  # strictly before
                    (df.index != idx)
                )
                
                past_avg = df.loc[mask, value_var].mean()
                df.at[idx, avg_col] = past_avg

            return df
        uw_ir_premium = compute_uw_past5y_avg(ipo_lead_matches, 'ipo_ir_premium')
        uw_vol_premium = compute_uw_past5y_avg(ipo_lead_matches, 'ipo_vol_premium')
        uw_sr_premium = compute_uw_past5y_avg(ipo_lead_matches, 'ipo_sr_premium')
        uw_performance = uw_ir_premium[['CUSIP','mng_prt_code','issue_date','uw_ipo_ir_premium']].copy()
        uw_performance = uw_performance.merge(uw_vol_premium[['CUSIP','mng_prt_code','issue_date','uw_ipo_vol_premium']],on=['CUSIP','mng_prt_code','issue_date'],how='left')
        uw_performance = uw_performance.merge(uw_sr_premium[['CUSIP','mng_prt_code','issue_date','uw_ipo_sr_premium']],on=['CUSIP','mng_prt_code','issue_date'],how='left')
        return uw_performance

    def describe_industry_focus_stats(self,all_matches,ipo_only=False,lead_only=False):
        if ipo_only:
            all_matches = all_matches[all_matches['ipo_flag'] == 1]
        if lead_only:
            all_matches = all_matches[all_matches['role'] == 'lead']
        industry_dist = all_matches.groupby(['mng_prt_code', 'ff12']).size().unstack(fill_value=0)
        ipo_count = industry_dist.sum(axis=1).rename('IPO_Count')
        industry_share = industry_dist.div(industry_dist.sum(axis=1), axis=0)
        hhi = (industry_share ** 2).sum(axis=1).rename('hhi').round(4)
        entropy = (-industry_share * np.log(industry_share + 1e-10)).sum(axis=1).rename('entropy').round(4)
        top_share = industry_share.max(axis=1).rename('top_share').round(4)
        top_share_industry = industry_share.idxmax(axis=1).rename('top_share_industry')
        focus_stats = pd.concat([hhi, entropy, top_share, top_share_industry, ipo_count], axis=1)
        return focus_stats
    
    def merge_uw_data(self,uw_rank,uw_share,uw_performance,firms_data,lead_match_data):
        uw_data = uw_rank[['mng_prt_code','issueyear','manager_name','rank']].merge(uw_share,on=['mng_prt_code','issueyear'],how='left')
        uw_perf_year = uw_performance.copy()
        uw_perf_year['issueyear'] = uw_perf_year['issue_date'].dt.year
        uw_perf_year = uw_perf_year.sort_values(by=['mng_prt_code','issue_date']).drop_duplicates(subset=['mng_prt_code','issueyear'],keep='last').drop(columns=['CUSIP','issue_date']).reset_index(drop=True)
        uw_data = uw_data.merge(uw_perf_year,on=['mng_prt_code','issueyear'],how='left')
        firms_matched = firms_data.merge(lead_match_data[lead_match_data['ipo_flag'] == 1][['CUSIP','issue_date','mng_prt_code','ff12']].drop_duplicates(subset=['CUSIP','issue_date'],keep='last'),on=['CUSIP','issue_date'],how='inner')
        firms_matched = firms_matched.merge(uw_data[['mng_prt_code','issueyear','manager_name','rank']],on=['mng_prt_code','issueyear'],how='left')
        firms_uw_avg = firms_matched.groupby(['mng_prt_code','issueyear'])[['VCinGC','firmage','as_analyst','asru_analyst','analyst']].mean().reset_index()
        uw_data = uw_data.merge(firms_uw_avg,on=['mng_prt_code','issueyear'],how='left')
        return uw_data
        
        
    
    
    
    def construct_data(self,cpi_excel_file='cpi.xlsx'):
        sdc_data = pd.read_pickle(self.data_path + 'sdc.pkl')
        sdc_us_common = sdc_data[(sdc_data['deal_type']=='US Common Stock')]
        sdc_ipo = sdc_us_common[(sdc_us_common['ipo_flag'] == 1)]
        missing_values_dfjr = ['.','-9','-99']
        ipoage = (pd.read_excel(self.data_path +"IPO-age.xlsx",na_values=missing_values_dfjr, dtype={'CUSIP': str})
        .iloc[:,:-3]
        #.drop(columns=['Rollup','Dual','PostIssueShares','Internet']) # the name PostIssueShares is new?
        .assign(CUSIP8=lambda d: d['CUSIP'])
        .assign(CUSIP=lambda d:d['CUSIP'].str[:6])
        .rename(columns={'Offer date':'issuedate','IPO name':'IPOname','CRSP permanent ID':'permno'})
        .assign(issuedate=lambda d:pd.to_datetime(d['issuedate'],format='%Y%m%d'))
        .assign(issueyear=lambda d:d['issuedate'].dt.year)
        .assign(firmage=lambda d:d['issueyear']-d['Founding'])
        .assign(VCinGC=lambda d:(d['VC']==1)|(d['VC']==2))
        .assign(VCinGC=lambda d:d['VCinGC']*1)
        .drop(columns='VC')
        .dropna(subset=['permno','CUSIP'])
        .reset_index(drop=True)
        )
        ipoage['issueyear'] = ipoage['issueyear'].astype(int)
        ipoage['firmage'] = np.where(ipoage['firmage']<0, np.nan, ipoage['firmage'])
        ipo_data = pd.merge(sdc_ipo,ipoage,how='inner',on='CUSIP')
        ipo_data.to_pickle(self.data_path+'ipo_data.pkl')
        print('ipo_data.pkl is saved at',self.data_path+'ipo_data.pkl')
        permnolist = ipo_data.permno.drop_duplicates().reset_index(drop=True)
        permnolist  = {'permnos':tuple(permnolist.astype(int))}
        conn = wrds.Connection(wrds_username=wrds_username,wrds_password=wrds_password)
        dsf_v2 = conn.raw_sql(
            """
            select permno,dlycaldt,dlyprc,dlyret,DlyClose,DlyPrcFlg,DlyDelFlg
            from crsp.dsf_v2
            where permno in %(permnos)s\
            """, 
            date_cols=['dlycaldt'],params=permnolist
        )
        dsf_v2['dlycaldt'] = pd.to_datetime(dsf_v2['dlycaldt'])
        dsi= conn.raw_sql(
            """
            select date,vwretx,ewretx
            from crsp.dsi
            """, 
            date_cols=['date']
        )
        dsi = dsi.rename(columns={'date':'dlycaldt'})
        dsf_v2 = dsf_v2.merge(dsi, on='dlycaldt', how='inner')
        dsf_v2.to_pickle(self.data_path+'dsf_v2.pkl')
        print('compa.pkl is saved at',self.data_path+'dsf_v2.pkl')
        link = conn.raw_sql(
            """
            select distinct gvkey, lpermno as permno, linkdt, linkenddt
            from crsp.Ccmxpf_linktable
            where linktype in ('LU', 'LC')
            and LINKPRIM in ('P', 'C')
            and lpermno in %(permnos)s\
            """,
            params=permnolist
        )
        link['gvkey'] = link.gvkey.astype(str)
        link['permno'] = link.permno.astype(int)
        # fill in missing end dates with a future date
        link['linkenddt'] = pd.to_datetime(link.linkenddt).fillna(pd.Timestamp('21000101'))
        gvkey_list = link['gvkey'].drop_duplicates().reset_index(drop=True)
        gvkey_list = {'gvkeys':tuple(gvkey_list.astype(str))}
        compq = conn.raw_sql(
            """
            select gvkey,datadate, atq,dlcq,revtq,oeps12,epspxq,fyr
            from comp.fundq 
            where datadate >= '1975-01-01' 
            and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
            and gvkey in %(gvkeys)s\
            order by gvkey, datadate
            """, 
            date_cols=['datadate'],params=gvkey_list
        )
        compa = conn.raw_sql(
            """
            select gvkey,datadate,fyr, at,dlc,revt,ebitda,sale,ebit
            from comp.funda 
            where datadate >= '1975-01-01' 
            and indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'
            and gvkey in %(gvkeys)s\
            order by gvkey, datadate
            """, 
            date_cols=['datadate'],params=gvkey_list
        )
        compa = compa.merge(link, on='gvkey', how='inner')
        compa = compa[(compa['linkdt']<=compa['datadate'])&(compa['datadate']<=compa['linkenddt'])]
        compa = compa.sort_values(by=['permno','datadate','linkdt']).drop_duplicates(subset=['permno','datadate'],keep='last')
        compa.to_pickle(self.data_path+'compa.pkl')
        print('compa.pkl is saved at',self.data_path+'compa.pkl')
        compq = compq.merge(link, on='gvkey', how='inner')
        compq = compq[(compq['linkdt']<=compq['datadate'])&(compq['datadate']<=compq['linkenddt'])]
        compq = compq.sort_values(by=['permno','datadate','linkdt']).drop_duplicates(subset=['permno','datadate'],keep='last')
        compq.to_pickle(self.data_path+'compq.pkl')
        print('compq.pkl is saved at',self.data_path+'compq.pkl')
        return None


    def process_ipo_data(self,ipo_data):
        
        # Step 2: Extract relevant columns for uw_code and filter
        uw_code = ipo_data[['lead_managers', 'number_managers', 'comanagers', 'mng_prt_code']].copy()
    #    uw_code = uw_code[uw_code['number_managers'] <= 10].copy()

        # Step 3: Create long-format dataframe for underwriter codes
        def create_long_format(row):
            lead_managers = str(row['lead_managers']).split(';') if pd.notna(row['lead_managers']) else []
            co_managers = str(row['comanagers']).split(';') if pd.notna(row['comanagers']) else []
            codes = str(row['mng_prt_code']).split('\n') if pd.notna(row['mng_prt_code']) else []

            all_managers = [(manager.strip(), 'lead') for manager in lead_managers] + \
                        [(manager.strip(), 'co') for manager in co_managers]

            rows = []
            for idx, (manager, role) in enumerate(all_managers):
                if idx < len(codes):
                    rows.append({'manager_name': manager, 'role': role, 'mng_prt_code': codes[idx]})
                else:
                    rows.append({'manager_name': manager, 'role': role, 'mng_prt_code': None})
            return rows

        long_format_rows = []
        for _, row in uw_code.iterrows():
            long_format_rows.extend(create_long_format(row))

        long_format_uw_code = pd.DataFrame(long_format_rows)

        # Step 4: Create IPO-firm-level long-format dataframe
        def create_firm_level_long_format(row):
            lead_managers = str(row['lead_managers']).split(';') if pd.notna(row['lead_managers']) else []
            co_managers = str(row['comanagers']).split(';') if pd.notna(row['comanagers']) else []

            all_managers = [(manager.strip(), 'lead') for manager in lead_managers] + \
                        [(manager.strip(), 'co') for manager in co_managers]

            rows = []
            for manager, role in all_managers:
                rows.append({
                    'CUSIP': row['CUSIP'],
                    'prt_cusip': row['prt_cusip'],
                    'ult_cusip': row['ult_cusip'],
                    'mainsiccode': row['mainsiccode'],
                    'issuedate': row['issuedate'],
                    'permno': row['permno'],
                    'IPOname': row['IPOname'],
                    'manager_name': manager,
                    'role': role
                })
            return rows

        firm_level_rows = []
        for _, row in ipo_data.iterrows():
            firm_level_rows.extend(create_firm_level_long_format(row))

        all_matches = pd.DataFrame(firm_level_rows)

        # Step 5: Merge the two long-format dataframes
        all_matches = all_matches.merge(
            long_format_uw_code,
            on=['manager_name', 'role'],
            how='left'
        )
        all_matches = all_matches.drop_duplicates().reset_index(drop=True)
        all_matches = all_matches.merge(all_matches.groupby(['CUSIP','issuedate'])['manager_name'].nunique().reset_index(name='num_managers'),on=['CUSIP','issuedate'])
        all_matches = all_matches.merge(all_matches.groupby(['CUSIP','issuedate'])['mng_prt_code'].nunique().reset_index(name='num_prt_codes'),on=['CUSIP','issuedate'])
        firm_lead_matches = all_matches[all_matches['role']=='lead'].merge(all_matches[all_matches['role']=='lead'].groupby(['CUSIP','issuedate'])['manager_name'].nunique().reset_index(name='num_leads'),on=['CUSIP','issuedate'])
        uw_code_list = long_format_uw_code[['manager_name','mng_prt_code']].drop_duplicates()
        uw_code_list = uw_code_list.sort_values(by=['mng_prt_code','manager_name']).reset_index(drop=True)
        print('UW code list saved to', self.data_path+'temp/uw_code_list.pkl')
        # Return the two resulting dataframes
        return all_matches,firm_lead_matches,uw_code_list

    def clean_name(name):
        common_terms = ["&", "co.", "inc.", "ltd.", "corp.", "corporation", "company", "inc", "ltd", "corp","llc","incorporated"]
        # Remove common terms
        name = re.sub(r'\b(?:' + '|'.join(map(re.escape, common_terms)) + r')\b', '', name, flags=re.IGNORECASE)
        # Replace "-" with a space and remove other punctuation
        name = re.sub(r'-', ' ', name)
        name = re.sub(r'[&.,]', '', name)
        # Remove extra spaces
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    def fuzzy_match(df1, df2, column1, column2, threshold=80):
        matches = []
        for name in df1[column1]:
            match = process.extractOne(name, df2[column2], scorer=fuzz.token_sort_ratio)
            if match and match[1] >= threshold:
                matches.append((name, match[0], match[1]))
        return pd.DataFrame(matches, columns=[column1, column2, 'Similarity'])

    def ipo_uw_match_data_process(self):
        uw_rank = pd.read_excel(self.data_path+'Underwriter-Rank.xls')
        uw_rank = uw_rank.drop(columns=['Unnamed: 13']).dropna(subset=['Underwriter Name']).drop_duplicates(subset=['Underwriter Name'])
        uw_rank = uw_rank[~uw_rank['Underwriter Name'].str.contains('no activity')]
        uw_rank_long = pd.wide_to_long(
            uw_rank,
            stubnames='Rank',  # Common prefix in column names
            i='Underwriter Name',  # Identifier column
            j='Year',  # Year suffix
            sep='',  # No separator between 'Rank' and year in column names
            suffix=r'\d+'
        ).reset_index()

        year_mapping = {
            8084: range(1980, 1985),
            8591: range(1985, 1992),
            9200: range(1992, 2001),
            104: range(2001, 2005),
            507: range(2005, 2008),
            809: range(2008, 2010),
            1011: range(2010, 2012),
            1217: range(2012, 2018),
            1820: range(2018, 2021),
            2122: range(2021, 2023),
            23: range(2023, 2024),
            24: range(2024, 2025),
        }

        # Create a dataframe with expanded years
        uw_rank_long = (
            uw_rank_long.assign(Year=uw_rank_long['Year'].map(year_mapping))  # Replace suffix with range
            .explode('Year')  # Expand rows for each year in the range
            .reset_index(drop=True)
        )
        uw_rank_long['Rank'] = pd.to_numeric(uw_rank_long['Rank'], errors='coerce')
        uw_rank_long['Rank'] = np.round(uw_rank_long['Rank'],0)
        uw_code_list = pd.read_csv(self.data_path+'temp/uw_code_list.csv')
        def clean_name(name):
            common_terms = ["&", "co.", "inc.", "ltd.", "corp.", "corporation", "company", "inc", "ltd", "corp","llc","incorporated"]
            # Remove common terms
            name = re.sub(r'\b(?:' + '|'.join(map(re.escape, common_terms)) + r')\b', '', name, flags=re.IGNORECASE)
            # Replace "-" with a space and remove other punctuation
            name = re.sub(r'-', ' ', name)
            name = re.sub(r'[&.,]', '', name)
            # Remove extra spaces
            name = re.sub(r'\s+', ' ', name).strip()
            return name
        def fuzzy_match(df1, df2, column1, column2, threshold=80):
            matches = []
            for name in df1[column1]:
                match = process.extractOne(name, df2[column2], scorer=fuzz.token_sort_ratio)
                if match and match[1] >= threshold:
                    matches.append((name, match[0], match[1]))
            return pd.DataFrame(matches, columns=[column1, column2, 'Similarity'])

        # Updated cleaning function to replace "-" with a space

    # Apply the updated cleaning function to both datasets
        uw_rank_name_list = uw_rank_long[['Underwriter Name','Rank8084']].drop_duplicates(subset=['Underwriter Name'])
        uw_rank_name_list['Underwriter Name Cleaned'] = uw_rank_name_list['Underwriter Name'].str.lower().str.strip().apply(clean_name)
        uw_code_list['manager_name_cleaned'] = uw_code_list['manager_name'].str.lower().str.strip().apply(clean_name)

        matched_names = fuzzy_match(uw_rank_name_list, uw_code_list, 'Underwriter Name Cleaned', 'manager_name_cleaned', threshold=80)
        uw_code_list = uw_code_list.merge(matched_names, left_on='manager_name_cleaned', right_on='manager_name_cleaned', how='left')
        uw_code_list = uw_code_list.merge(uw_rank_name_list[['Underwriter Name','Underwriter Name Cleaned']],on='Underwriter Name Cleaned',how='left')
        manual_match_table = pd.read_excel(self.data_path+'uw_code_rank_manual_match.xlsx')
        manual_match_table['revised'] = 1.0
        uw_code_list = uw_code_list.merge(manual_match_table[['manager_name','Underwriter Name','revised']],left_on='manager_name',right_on='manager_name',how='left')
        uw_code_list['Underwriter Name'] = uw_code_list['Underwriter Name_y'].fillna(uw_code_list['Underwriter Name_x'])
        uw_code_list['Underwriter Name'] = np.where(uw_code_list['revised']==1.0,uw_code_list['Underwriter Name_y'],uw_code_list['Underwriter Name'])
        uw_code_list['revised'] = np.where(uw_code_list['revised']==1.0,1.0,0.0)
        uw_code_list = uw_code_list.sort_values(by=['manager_name','revised']).drop_duplicates(subset=['manager_name'],keep='last').reset_index(drop=True)
        uw_code_list = uw_code_list[['manager_name','mng_prt_code','Underwriter Name']]
        ipo_uw_match = pd.read_pickle(self.data_path+'merged_df.pkl')
        ipo_uw_match = ipo_uw_match[ipo_uw_match['role']=='lead'].copy()
        ipo_uw_match['issueyear'] = ipo_uw_match['issuedate'].dt.year
        uw_rank_year = ipo_uw_match[['manager_name','mng_prt_code','issueyear']].drop_duplicates(subset=['manager_name','issueyear'])
        uw_rank_year = uw_rank_year.drop(columns=['mng_prt_code']).merge(uw_code_list,on='manager_name',how='left')
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Barclays'))&(uw_rank_year['issueyear']>=1992)&(uw_rank_year['issueyear']<=2000), 'Barclay Investments, Inc.', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Barclays'))&(uw_rank_year['issueyear']>=2005), 'Barclays Capital', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Banco BTG Pactual SA'), 'BTG Pactual', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Cantor Fitzgerald Inc'), 'Cantor, Fitzgerald & Co., Inc.', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='China International Capital Corp HK Securities Ltd'), 'China International Capital Corp (CICC)', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Citi'), 'Citigroup', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('China Renaissance'))&(uw_rank_year['issueyear']>=2012), 'China Renaissance (CRS)', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Berenberg Capital Markets LLC'), 'Berenberg', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Mizuho Securities')), 'Mizuho Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Nomura Securities International Inc'), 'Nomura Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Cowen Securities LLC'), 'Cowen', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Credit Suisse'))&(uw_rank_year['issueyear']>=1992)&(uw_rank_year['issueyear']<=2000), 'Credit Suisse First Boston', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Credit Suisse'))&(uw_rank_year['issueyear']>=1992)&(uw_rank_year['issueyear']<=2011), 'Credit Suisse First Boston', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Credit Suisse'))&(uw_rank_year['issueyear']>=2012), 'Credit Suisse', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('UBS'))&(uw_rank_year['issueyear']>=2001), 'UBS Investment Bank', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('UBS'))&(uw_rank_year['issueyear']>=1992)&(uw_rank_year['issueyear']<=2000), 'UBS Ltd', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='ViewTrade Securities Inc'), 'VIEWT-SEC', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Wedbush Securities, Inc.')&(uw_rank_year['issueyear']>=2005), 'Wedbush Morgan Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Merrill Lynch Pierce Fenner & Smith')&(uw_rank_year['issueyear']>=2005), 'Merrill Lynch & Co Inc', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='BofA Securities Inc'), 'Banc of America Securities LLC', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='BA Securities Inc')&(uw_rank_year['issueyear']>=2001), 'Banc of America Securities LLC', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Boustead Securities LLC'), 'Boustead Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='GKN Securities Corp'), 'GKN Securities Corp', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('JP Morgan'))&(uw_rank_year['issueyear']>=2005), 'JP Morgan (JPM)', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Northland Securities Inc'), 'Northland-Sec', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Piper Jaffray Cos')&(uw_rank_year['issueyear']>=2005), 'Piper Jaffray Inc', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Santander Investment Securities Inc'), 'Santander Investment Bank', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Sentra Securities Corp'), 'Sentra Securities', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('HSBC'))&(uw_rank_year['issueyear']>=2010), 'HSBC', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name'].str.contains('Leerink'))&(uw_rank_year['issueyear']<=2020), 'Leerink Swann & Co.', uw_rank_year['Underwriter Name'])
        uw_rank_year['Underwriter Name'] = np.where((uw_rank_year['manager_name']=='Robinson-Humphrey Co')&(uw_rank_year['issueyear']<=2000), 'Robinson-Humphrey (Old)', uw_rank_year['Underwriter Name'])
        uw_rank_year = uw_rank_year.merge(uw_rank_long.rename(columns={'Rank':'rank'}),left_on=['Underwriter Name','issueyear'],right_on=['Underwriter Name','Year'],how='left')
        uw_rank_year['rank'] = np.where(uw_rank_year['rank']<0, np.nan, uw_rank_year['rank'])

        uw_rank_year.sort_values(by=['mng_prt_code','issueyear','manager_name']).to_csv(self.data_path+'uw_rank_year.csv',index=False)
        ipo_uw_match = ipo_uw_match.merge(uw_rank_year[['manager_name','issueyear','rank']],on=['manager_name','issueyear'],how='left')
        ipo_uw_match.to_pickle(self.data_path+'temp/ipo_uw_match.pkl')
        print('IPO-UW match saved to', self.data_path+'ipo_uw_match.pkl')
        return ipo_uw_match