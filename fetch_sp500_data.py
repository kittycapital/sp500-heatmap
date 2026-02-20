"""
S&P 500 / QQQ / DIA Heatmap Data Fetcher (통합 버전)
yfinance로 S&P 500, Nasdaq 100, Dow 30 종목 데이터 통합 수집
겹치는 종목은 한 번만 API 호출하여 효율적으로 처리
GitHub Actions에서 매일 한국시간 06:30 (UTC 21:30) 실행
"""

import yfinance as yf
import json
import os
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

OUTPUT_DIR = "data"

# ============================================================
# S&P 500 구성종목 (2026-02 기준, Wikipedia 기반)
# {ticker: (name, sector)}
# ============================================================
SP500 = {
    'MMM': ('3M', 'Industrials'),
    'AOS': ('A. O. Smith', 'Industrials'),
    'ABT': ('Abbott Laboratories', 'Health Care'),
    'ABBV': ('AbbVie', 'Health Care'),
    'ACN': ('Accenture', 'Information Technology'),
    'ADBE': ('Adobe Inc.', 'Information Technology'),
    'AMD': ('Advanced Micro Devices', 'Information Technology'),
    'AES': ('AES Corporation', 'Utilities'),
    'AFL': ('Aflac', 'Financials'),
    'A': ('Agilent Technologies', 'Health Care'),
    'APD': ('Air Products', 'Materials'),
    'ABNB': ('Airbnb', 'Consumer Discretionary'),
    'AKAM': ('Akamai Technologies', 'Information Technology'),
    'ALB': ('Albemarle Corporation', 'Materials'),
    'ARE': ('Alexandria Real Estate Equities', 'Real Estate'),
    'ALGN': ('Align Technology', 'Health Care'),
    'ALLE': ('Allegion', 'Industrials'),
    'LNT': ('Alliant Energy', 'Utilities'),
    'ALL': ('Allstate', 'Financials'),
    'GOOGL': ('Alphabet Inc. (Class A)', 'Communication Services'),
    'GOOG': ('Alphabet Inc. (Class C)', 'Communication Services'),
    'MO': ('Altria', 'Consumer Staples'),
    'AMZN': ('Amazon', 'Consumer Discretionary'),
    'AMCR': ('Amcor', 'Materials'),
    'AEE': ('Ameren', 'Utilities'),
    'AEP': ('American Electric Power', 'Utilities'),
    'AXP': ('American Express', 'Financials'),
    'AIG': ('American International Group', 'Financials'),
    'AMT': ('American Tower', 'Real Estate'),
    'AWK': ('American Water Works', 'Utilities'),
    'AMP': ('Ameriprise Financial', 'Financials'),
    'AME': ('Ametek', 'Industrials'),
    'AMGN': ('Amgen', 'Health Care'),
    'APH': ('Amphenol', 'Information Technology'),
    'ADI': ('Analog Devices', 'Information Technology'),
    'AON': ('Aon plc', 'Financials'),
    'APA': ('APA Corporation', 'Energy'),
    'APO': ('Apollo Global Management', 'Financials'),
    'AAPL': ('Apple Inc.', 'Information Technology'),
    'AMAT': ('Applied Materials', 'Information Technology'),
    'APP': ('AppLovin', 'Information Technology'),
    'APTV': ('Aptiv', 'Consumer Discretionary'),
    'ACGL': ('Arch Capital Group', 'Financials'),
    'ADM': ('Archer Daniels Midland', 'Consumer Staples'),
    'ARES': ('Ares Management', 'Financials'),
    'ANET': ('Arista Networks', 'Information Technology'),
    'AJG': ('Arthur J. Gallagher & Co.', 'Financials'),
    'AIZ': ('Assurant', 'Financials'),
    'T': ('AT&T', 'Communication Services'),
    'ATO': ('Atmos Energy', 'Utilities'),
    'ADSK': ('Autodesk', 'Information Technology'),
    'ADP': ('Automatic Data Processing', 'Industrials'),
    'AZO': ('AutoZone', 'Consumer Discretionary'),
    'AVB': ('AvalonBay Communities', 'Real Estate'),
    'AVY': ('Avery Dennison', 'Materials'),
    'AXON': ('Axon Enterprise', 'Industrials'),
    'BKR': ('Baker Hughes', 'Energy'),
    'BALL': ('Ball Corporation', 'Materials'),
    'BAC': ('Bank of America', 'Financials'),
    'BAX': ('Baxter International', 'Health Care'),
    'BDX': ('Becton Dickinson', 'Health Care'),
    'BRK-B': ('Berkshire Hathaway', 'Financials'),
    'BBY': ('Best Buy', 'Consumer Discretionary'),
    'TECH': ('Bio-Techne', 'Health Care'),
    'BIIB': ('Biogen', 'Health Care'),
    'BLK': ('BlackRock', 'Financials'),
    'BX': ('Blackstone Inc.', 'Financials'),
    'XYZ': ('Block, Inc.', 'Financials'),
    'BK': ('BNY Mellon', 'Financials'),
    'BA': ('Boeing', 'Industrials'),
    'BKNG': ('Booking Holdings', 'Consumer Discretionary'),
    'BSX': ('Boston Scientific', 'Health Care'),
    'BMY': ('Bristol Myers Squibb', 'Health Care'),
    'AVGO': ('Broadcom', 'Information Technology'),
    'BR': ('Broadridge Financial Solutions', 'Industrials'),
    'BRO': ('Brown & Brown', 'Financials'),
    'BF-B': ('Brown-Forman', 'Consumer Staples'),
    'BLDR': ('Builders FirstSource', 'Industrials'),
    'BG': ('Bunge Global', 'Consumer Staples'),
    'BXP': ('BXP, Inc.', 'Real Estate'),
    'CHRW': ('C.H. Robinson', 'Industrials'),
    'CDNS': ('Cadence Design Systems', 'Information Technology'),
    'CPT': ('Camden Property Trust', 'Real Estate'),
    'CPB': ("Campbell's Company", 'Consumer Staples'),
    'COF': ('Capital One', 'Financials'),
    'CAH': ('Cardinal Health', 'Health Care'),
    'CCL': ('Carnival', 'Consumer Discretionary'),
    'CARR': ('Carrier Global', 'Industrials'),
    'CVNA': ('Carvana', 'Consumer Discretionary'),
    'CAT': ('Caterpillar Inc.', 'Industrials'),
    'CBOE': ('Cboe Global Markets', 'Financials'),
    'CBRE': ('CBRE Group', 'Real Estate'),
    'CDW': ('CDW Corporation', 'Information Technology'),
    'COR': ('Cencora', 'Health Care'),
    'CNC': ('Centene Corporation', 'Health Care'),
    'CNP': ('CenterPoint Energy', 'Utilities'),
    'CF': ('CF Industries', 'Materials'),
    'CRL': ('Charles River Laboratories', 'Health Care'),
    'SCHW': ('Charles Schwab Corporation', 'Financials'),
    'CHTR': ('Charter Communications', 'Communication Services'),
    'CVX': ('Chevron Corporation', 'Energy'),
    'CMG': ('Chipotle Mexican Grill', 'Consumer Discretionary'),
    'CB': ('Chubb Limited', 'Financials'),
    'CHD': ('Church & Dwight', 'Consumer Staples'),
    'CIEN': ('Ciena', 'Information Technology'),
    'CI': ('Cigna', 'Health Care'),
    'CINF': ('Cincinnati Financial', 'Financials'),
    'CTAS': ('Cintas', 'Industrials'),
    'CSCO': ('Cisco', 'Information Technology'),
    'C': ('Citigroup', 'Financials'),
    'CFG': ('Citizens Financial Group', 'Financials'),
    'CLX': ('Clorox', 'Consumer Staples'),
    'CME': ('CME Group', 'Financials'),
    'CMS': ('CMS Energy', 'Utilities'),
    'KO': ('Coca-Cola Company', 'Consumer Staples'),
    'CTSH': ('Cognizant', 'Information Technology'),
    'COIN': ('Coinbase', 'Financials'),
    'CL': ('Colgate-Palmolive', 'Consumer Staples'),
    'CMCSA': ('Comcast', 'Communication Services'),
    'FIX': ('Comfort Systems USA', 'Industrials'),
    'CAG': ('Conagra Brands', 'Consumer Staples'),
    'COP': ('ConocoPhillips', 'Energy'),
    'ED': ('Consolidated Edison', 'Utilities'),
    'STZ': ('Constellation Brands', 'Consumer Staples'),
    'CEG': ('Constellation Energy', 'Utilities'),
    'COO': ('Cooper Companies', 'Health Care'),
    'CPRT': ('Copart', 'Industrials'),
    'GLW': ('Corning Inc.', 'Information Technology'),
    'CPAY': ('Corpay', 'Financials'),
    'CTVA': ('Corteva', 'Materials'),
    'CSGP': ('CoStar Group', 'Real Estate'),
    'COST': ('Costco', 'Consumer Staples'),
    'CTRA': ('Coterra', 'Energy'),
    'CRH': ('CRH plc', 'Materials'),
    'CRWD': ('CrowdStrike', 'Information Technology'),
    'CCI': ('Crown Castle', 'Real Estate'),
    'CSX': ('CSX Corporation', 'Industrials'),
    'CMI': ('Cummins', 'Industrials'),
    'CVS': ('CVS Health', 'Health Care'),
    'DHR': ('Danaher Corporation', 'Health Care'),
    'DRI': ('Darden Restaurants', 'Consumer Discretionary'),
    'DDOG': ('Datadog', 'Information Technology'),
    'DVA': ('DaVita', 'Health Care'),
    'DECK': ('Deckers Brands', 'Consumer Discretionary'),
    'DE': ('Deere & Company', 'Industrials'),
    'DELL': ('Dell Technologies', 'Information Technology'),
    'DAL': ('Delta Air Lines', 'Industrials'),
    'DVN': ('Devon Energy', 'Energy'),
    'DXCM': ('Dexcom', 'Health Care'),
    'FANG': ('Diamondback Energy', 'Energy'),
    'DLR': ('Digital Realty', 'Real Estate'),
    'DG': ('Dollar General', 'Consumer Staples'),
    'DLTR': ('Dollar Tree', 'Consumer Staples'),
    'D': ('Dominion Energy', 'Utilities'),
    'DPZ': ("Domino's", 'Consumer Discretionary'),
    'DASH': ('DoorDash', 'Consumer Discretionary'),
    'DOV': ('Dover Corporation', 'Industrials'),
    'DOW': ('Dow Inc.', 'Materials'),
    'DHI': ('D. R. Horton', 'Consumer Discretionary'),
    'DTE': ('DTE Energy', 'Utilities'),
    'DUK': ('Duke Energy', 'Utilities'),
    'DD': ('DuPont', 'Materials'),
    'ETN': ('Eaton Corporation', 'Industrials'),
    'EBAY': ('eBay Inc.', 'Consumer Discretionary'),
    'ECL': ('Ecolab', 'Materials'),
    'EIX': ('Edison International', 'Utilities'),
    'EW': ('Edwards Lifesciences', 'Health Care'),
    'EA': ('Electronic Arts', 'Communication Services'),
    'ELV': ('Elevance Health', 'Health Care'),
    'EME': ('Emcor', 'Industrials'),
    'EMR': ('Emerson Electric', 'Industrials'),
    'ETR': ('Entergy', 'Utilities'),
    'EOG': ('EOG Resources', 'Energy'),
    'EPAM': ('EPAM Systems', 'Information Technology'),
    'EQT': ('EQT Corporation', 'Energy'),
    'EFX': ('Equifax', 'Industrials'),
    'EQIX': ('Equinix', 'Real Estate'),
    'EQR': ('Equity Residential', 'Real Estate'),
    'ERIE': ('Erie Indemnity', 'Financials'),
    'ESS': ('Essex Property Trust', 'Real Estate'),
    'EL': ('Estee Lauder', 'Consumer Staples'),
    'EG': ('Everest Group', 'Financials'),
    'EVRG': ('Evergy', 'Utilities'),
    'ES': ('Eversource Energy', 'Utilities'),
    'EXC': ('Exelon', 'Utilities'),
    'EXE': ('Expand Energy', 'Energy'),
    'EXPD': ('Expeditors International', 'Industrials'),
    'EXR': ('Extra Space Storage', 'Real Estate'),
    'XOM': ('ExxonMobil', 'Energy'),
    'FFIV': ('F5 Networks', 'Information Technology'),
    'FANG': ('Diamondback Energy', 'Energy'),
    'FAST': ('Fastenal', 'Industrials'),
    'FRT': ('Federal Realty Investment Trust', 'Real Estate'),
    'FDX': ('FedEx', 'Industrials'),
    'FIS': ('Fidelity National Information Services', 'Financials'),
    'FITB': ('Fifth Third Bancorp', 'Financials'),
    'FICO': ('Fair Isaac Corporation', 'Information Technology'),
    'FE': ('FirstEnergy', 'Utilities'),
    'FISV': ('Fiserv', 'Financials'),
    'FI': ('Fiserv', 'Financials'),
    'F': ('Ford Motor Company', 'Consumer Discretionary'),
    'FTNT': ('Fortinet', 'Information Technology'),
    'FTV': ('Fortive', 'Industrials'),
    'FOXA': ('Fox Corporation (Class A)', 'Communication Services'),
    'FOX': ('Fox Corporation (Class B)', 'Communication Services'),
    'BEN': ('Franklin Templeton', 'Financials'),
    'FCX': ('Freeport-McMoRan', 'Materials'),
    'FSLR': ('First Solar', 'Information Technology'),
    'GRMN': ('Garmin', 'Consumer Discretionary'),
    'IT': ('Gartner', 'Information Technology'),
    'GEHC': ('GE HealthCare', 'Health Care'),
    'GE': ('GE Aerospace', 'Industrials'),
    'GEN': ('Gen Digital', 'Information Technology'),
    'GEV': ('GE Vernova', 'Industrials'),
    'GNRC': ('Generac', 'Industrials'),
    'GD': ('General Dynamics', 'Industrials'),
    'GIS': ('General Mills', 'Consumer Staples'),
    'GM': ('General Motors', 'Consumer Discretionary'),
    'GILD': ('Gilead Sciences', 'Health Care'),
    'GPN': ('Global Payments', 'Financials'),
    'GL': ('Globe Life', 'Financials'),
    'GDDY': ('GoDaddy', 'Information Technology'),
    'GS': ('Goldman Sachs', 'Financials'),
    'HAL': ('Halliburton', 'Energy'),
    'HAS': ('Hasbro', 'Consumer Discretionary'),
    'HCA': ('HCA Healthcare', 'Health Care'),
    'DOC': ('Healthpeak Properties', 'Real Estate'),
    'HSIC': ('Henry Schein', 'Health Care'),
    'HSY': ('Hershey Company', 'Consumer Staples'),
    'HES': ('Hess Corporation', 'Energy'),
    'HPE': ('Hewlett Packard Enterprise', 'Information Technology'),
    'HLT': ('Hilton Worldwide', 'Consumer Discretionary'),
    'HOLX': ('Hologic', 'Health Care'),
    'HD': ('Home Depot', 'Consumer Discretionary'),
    'HON': ('Honeywell', 'Industrials'),
    'HRL': ('Hormel Foods', 'Consumer Staples'),
    'HST': ('Host Hotels & Resorts', 'Real Estate'),
    'HWM': ('Howmet Aerospace', 'Industrials'),
    'HPQ': ('HP Inc.', 'Information Technology'),
    'HUBB': ('Hubbell', 'Industrials'),
    'HUM': ('Humana', 'Health Care'),
    'HII': ('Huntington Ingalls Industries', 'Industrials'),
    'HBAN': ('Huntington Bancshares', 'Financials'),
    'IBM': ('IBM', 'Information Technology'),
    'IEX': ('Idex Corporation', 'Industrials'),
    'IDXX': ('Idexx Laboratories', 'Health Care'),
    'IFF': ('International Flavors & Fragrances', 'Materials'),
    'INCY': ('Incyte', 'Health Care'),
    'IR': ('Ingersoll Rand', 'Industrials'),
    'INTC': ('Intel', 'Information Technology'),
    'ICE': ('Intercontinental Exchange', 'Financials'),
    'IP': ('International Paper', 'Materials'),
    'INTU': ('Intuit', 'Information Technology'),
    'ISRG': ('Intuitive Surgical', 'Health Care'),
    'IVZ': ('Invesco', 'Financials'),
    'INVH': ('Invitation Homes', 'Real Estate'),
    'IQV': ('IQVIA', 'Health Care'),
    'IRM': ('Iron Mountain', 'Real Estate'),
    'ITW': ('Illinois Tool Works', 'Industrials'),
    'J': ('Jacobs Solutions', 'Industrials'),
    'JBHT': ('J.B. Hunt Transport Services', 'Industrials'),
    'JBL': ('Jabil', 'Information Technology'),
    'JCI': ('Johnson Controls', 'Industrials'),
    'JNJ': ('Johnson & Johnson', 'Health Care'),
    'JPM': ('JPMorgan Chase', 'Financials'),
    'JKHY': ('Jack Henry & Associates', 'Financials'),
    'KDP': ('Keurig Dr Pepper', 'Consumer Staples'),
    'KEY': ('KeyCorp', 'Financials'),
    'KEYS': ('Keysight Technologies', 'Information Technology'),
    'KHC': ('Kraft Heinz', 'Consumer Staples'),
    'KIM': ('Kimco Realty', 'Real Estate'),
    'KMI': ('Kinder Morgan', 'Energy'),
    'KKR': ('KKR & Co.', 'Financials'),
    'KLAC': ('KLA Corporation', 'Information Technology'),
    'KMB': ('Kimberly-Clark', 'Consumer Staples'),
    'KR': ('Kroger', 'Consumer Staples'),
    'KVUE': ('Kenvue', 'Consumer Staples'),
    'LH': ('Labcorp', 'Health Care'),
    'LRCX': ('Lam Research', 'Information Technology'),
    'LW': ('Lamb Weston', 'Consumer Staples'),
    'L': ('Loews Corporation', 'Financials'),
    'LDOS': ('Leidos', 'Industrials'),
    'LEN': ('Lennar', 'Consumer Discretionary'),
    'LII': ('Lennox International', 'Industrials'),
    'LLY': ('Lilly (Eli)', 'Health Care'),
    'LIN': ('Linde plc', 'Materials'),
    'LYV': ('Live Nation Entertainment', 'Communication Services'),
    'LMT': ('Lockheed Martin', 'Industrials'),
    'LOW': ('Lowe\'s', 'Consumer Discretionary'),
    'LULU': ('Lululemon', 'Consumer Discretionary'),
    'LVS': ('Las Vegas Sands', 'Consumer Discretionary'),
    'LYB': ('LyondellBasell', 'Materials'),
    'MCK': ('McKesson', 'Health Care'),
    'MCD': ('McDonald\'s', 'Consumer Discretionary'),
    'MDT': ('Medtronic', 'Health Care'),
    'MRK': ('Merck & Co.', 'Health Care'),
    'META': ('Meta Platforms', 'Communication Services'),
    'MET': ('MetLife', 'Financials'),
    'MTD': ('Mettler-Toledo', 'Health Care'),
    'MGM': ('MGM Resorts', 'Consumer Discretionary'),
    'MCHP': ('Microchip Technology', 'Information Technology'),
    'MU': ('Micron Technology', 'Information Technology'),
    'MSFT': ('Microsoft', 'Information Technology'),
    'MAA': ('Mid-America Apartment', 'Real Estate'),
    'MRNA': ('Moderna', 'Health Care'),
    'MHK': ('Mohawk Industries', 'Consumer Discretionary'),
    'MOH': ('Molina Healthcare', 'Health Care'),
    'TAP': ('Molson Coors Beverage Company', 'Consumer Staples'),
    'MDLZ': ('Mondelez International', 'Consumer Staples'),
    'MPWR': ('Monolithic Power Systems', 'Information Technology'),
    'MNST': ('Monster Beverage', 'Consumer Staples'),
    'MCO': ('Moody\'s', 'Financials'),
    'MS': ('Morgan Stanley', 'Financials'),
    'MOS': ('Mosaic Company', 'Materials'),
    'MSI': ('Motorola Solutions', 'Information Technology'),
    'MSCI': ('MSCI Inc.', 'Financials'),
    'MRSH': ('Marsh McLennan', 'Financials'),
    'NDAQ': ('Nasdaq Inc.', 'Financials'),
    'NDSN': ('Nordson', 'Industrials'),
    'NSC': ('Norfolk Southern', 'Industrials'),
    'NTRS': ('Northern Trust', 'Financials'),
    'NOC': ('Northrop Grumman', 'Industrials'),
    'NVDA': ('Nvidia', 'Information Technology'),
    'NRG': ('NRG Energy', 'Utilities'),
    'NUE': ('Nucor', 'Materials'),
    'NTAP': ('NetApp', 'Information Technology'),
    'NFLX': ('Netflix', 'Communication Services'),
    'NEM': ('Newmont', 'Materials'),
    'NEE': ('NextEra Energy', 'Utilities'),
    'NI': ('NiSource', 'Utilities'),
    'NKE': ('Nike', 'Consumer Discretionary'),
    'NWSA': ('News Corp (Class A)', 'Communication Services'),
    'NWS': ('News Corp (Class B)', 'Communication Services'),
    'NOW': ('ServiceNow', 'Information Technology'),
    'NXPI': ('NXP Semiconductors', 'Information Technology'),
    'O': ('Realty Income', 'Real Estate'),
    'ODFL': ('Old Dominion Freight Line', 'Industrials'),
    'OKE': ('ONEOK', 'Energy'),
    'ORCL': ('Oracle Corporation', 'Information Technology'),
    'ORLY': ("O'Reilly Automotive", 'Consumer Discretionary'),
    'OTIS': ('Otis Worldwide', 'Industrials'),
    'OXY': ('Occidental Petroleum', 'Energy'),
    'OMC': ('Omnicom Group', 'Communication Services'),
    'ON': ('ON Semiconductor', 'Information Technology'),
    'PCAR': ('Paccar', 'Industrials'),
    'PKG': ('Packaging Corporation of America', 'Materials'),
    'PLTR': ('Palantir Technologies', 'Information Technology'),
    'PANW': ('Palo Alto Networks', 'Information Technology'),
    'PARA': ('Paramount Global', 'Communication Services'),
    'PH': ('Parker Hannifin', 'Industrials'),
    'PAYX': ('Paychex', 'Industrials'),
    'PAYC': ('Paycom', 'Industrials'),
    'PYPL': ('PayPal', 'Financials'),
    'PNR': ('Pentair', 'Industrials'),
    'PEP': ('PepsiCo', 'Consumer Staples'),
    'PFE': ('Pfizer', 'Health Care'),
    'PCG': ('PG&E Corporation', 'Utilities'),
    'PM': ('Philip Morris International', 'Consumer Staples'),
    'PFG': ('Principal Financial Group', 'Financials'),
    'PG': ('Procter & Gamble', 'Consumer Staples'),
    'PGR': ('Progressive Corporation', 'Financials'),
    'PLD': ('Prologis', 'Real Estate'),
    'PRU': ('Prudential Financial', 'Financials'),
    'PSA': ('Public Storage', 'Real Estate'),
    'PTC': ('PTC Inc.', 'Information Technology'),
    'PEG': ('Public Service Enterprise Group', 'Utilities'),
    'PNW': ('Pinnacle West Capital', 'Utilities'),
    'PPL': ('PPL Corporation', 'Utilities'),
    'PPG': ('PPG Industries', 'Materials'),
    'PSX': ('Phillips 66', 'Energy'),
    'PHM': ('PulteGroup', 'Consumer Discretionary'),
    'QCOM': ('Qualcomm', 'Information Technology'),
    'Q': ('Quintiles IMS Holdings', 'Information Technology'),
    'PWR': ('Quanta Services', 'Industrials'),
    'DGX': ('Quest Diagnostics', 'Health Care'),
    'RL': ('Ralph Lauren', 'Consumer Discretionary'),
    'RJF': ('Raymond James Financial', 'Financials'),
    'RTX': ('RTX Corporation', 'Industrials'),
    'RMD': ('ResMed', 'Health Care'),
    'REG': ('Regency Centers', 'Real Estate'),
    'REGN': ('Regeneron', 'Health Care'),
    'RF': ('Regions Financial', 'Financials'),
    'RSG': ('Republic Services', 'Industrials'),
    'RCL': ('Royal Caribbean', 'Consumer Discretionary'),
    'RIVN': ('Rivian Automotive', 'Consumer Discretionary'),
    'ROK': ('Rockwell Automation', 'Industrials'),
    'ROL': ('Rollins', 'Industrials'),
    'ROP': ('Roper Technologies', 'Industrials'),
    'ROST': ('Ross Stores', 'Consumer Discretionary'),
    'RHI': ('Robert Half International', 'Industrials'),
    'SBAC': ('SBA Communications', 'Real Estate'),
    'SLB': ('Schlumberger', 'Energy'),
    'STX': ('Seagate Technology', 'Information Technology'),
    'SRE': ('Sempra Energy', 'Utilities'),
    'SHW': ('Sherwin-Williams', 'Materials'),
    'SPG': ('Simon Property Group', 'Real Estate'),
    'SMCI': ('Super Micro Computer', 'Information Technology'),
    'SJM': ('J.M. Smucker Company', 'Consumer Staples'),
    'SNA': ('Snap-on', 'Industrials'),
    'SNPS': ('Synopsys', 'Information Technology'),
    'SO': ('Southern Company', 'Utilities'),
    'SW': ('Smurfit Westrock', 'Materials'),
    'SWK': ('Stanley Black & Decker', 'Industrials'),
    'SBUX': ('Starbucks', 'Consumer Discretionary'),
    'STLD': ('Steel Dynamics', 'Materials'),
    'STE': ('Steris', 'Health Care'),
    'STT': ('State Street', 'Financials'),
    'SOLV': ('Solventum', 'Health Care'),
    'SYK': ('Stryker', 'Health Care'),
    'SYF': ('Synchrony Financial', 'Financials'),
    'SNDK': ('SanDisk', 'Information Technology'),
    'SYY': ('Sysco', 'Consumer Staples'),
    'TMUS': ('T-Mobile US', 'Communication Services'),
    'TRGP': ('Targa Resources', 'Energy'),
    'TGT': ('Target', 'Consumer Staples'),
    'TDY': ('Teledyne Technologies', 'Information Technology'),
    'TFX': ('Teleflex', 'Health Care'),
    'TEL': ('TE Connectivity', 'Information Technology'),
    'TER': ('Teradyne', 'Information Technology'),
    'TSLA': ('Tesla, Inc.', 'Consumer Discretionary'),
    'TXN': ('Texas Instruments', 'Information Technology'),
    'TPL': ('Texas Pacific Land', 'Energy'),
    'TXT': ('Textron', 'Industrials'),
    'TMO': ('Thermo Fisher Scientific', 'Health Care'),
    'TJX': ('TJX Companies', 'Consumer Discretionary'),
    'TSCO': ('Tractor Supply Company', 'Consumer Discretionary'),
    'TT': ('Trane Technologies', 'Industrials'),
    'TDG': ('TransDigm Group', 'Industrials'),
    'TRV': ('Travelers Companies', 'Financials'),
    'TRMB': ('Trimble', 'Information Technology'),
    'TFC': ('Truist Financial', 'Financials'),
    'TSN': ('Tyson Foods', 'Consumer Staples'),
    'TYL': ('Tyler Technologies', 'Information Technology'),
    'USB': ('U.S. Bancorp', 'Financials'),
    'UBER': ('Uber', 'Industrials'),
    'UDR': ('UDR, Inc.', 'Real Estate'),
    'ULTA': ('Ulta Beauty', 'Consumer Discretionary'),
    'UNP': ('Union Pacific Corporation', 'Industrials'),
    'UAL': ('United Airlines Holdings', 'Industrials'),
    'UPS': ('United Parcel Service', 'Industrials'),
    'URI': ('United Rentals', 'Industrials'),
    'UNH': ('UnitedHealth Group', 'Health Care'),
    'UHS': ('Universal Health Services', 'Health Care'),
    'VLO': ('Valero Energy', 'Energy'),
    'VTR': ('Ventas', 'Real Estate'),
    'VLTO': ('Veralto', 'Industrials'),
    'VRSN': ('Verisign', 'Information Technology'),
    'VRSK': ('Verisk Analytics', 'Industrials'),
    'VZ': ('Verizon', 'Communication Services'),
    'VRTX': ('Vertex Pharmaceuticals', 'Health Care'),
    'VTRS': ('Viatris', 'Health Care'),
    'VICI': ('Vici Properties', 'Real Estate'),
    'V': ('Visa Inc.', 'Financials'),
    'VST': ('Vistra Corp.', 'Utilities'),
    'VMC': ('Vulcan Materials Company', 'Materials'),
    'WRB': ('W. R. Berkley Corporation', 'Financials'),
    'GWW': ('W. W. Grainger', 'Industrials'),
    'WAB': ('Wabtec', 'Industrials'),
    'WMT': ('Walmart', 'Consumer Staples'),
    'DIS': ('Walt Disney Company', 'Communication Services'),
    'WBD': ('Warner Bros. Discovery', 'Communication Services'),
    'WM': ('Waste Management', 'Industrials'),
    'WAT': ('Waters Corporation', 'Health Care'),
    'WEC': ('WEC Energy Group', 'Utilities'),
    'WFC': ('Wells Fargo', 'Financials'),
    'WELL': ('Welltower', 'Real Estate'),
    'WST': ('West Pharmaceutical Services', 'Health Care'),
    'WDC': ('Western Digital', 'Information Technology'),
    'WY': ('Weyerhaeuser', 'Real Estate'),
    'WSM': ('Williams-Sonoma, Inc.', 'Consumer Discretionary'),
    'WMB': ('Williams Companies', 'Energy'),
    'WTW': ('Willis Towers Watson', 'Financials'),
    'WDAY': ('Workday, Inc.', 'Information Technology'),
    'WYNN': ('Wynn Resorts', 'Consumer Discretionary'),
    'XEL': ('Xcel Energy', 'Utilities'),
    'XYL': ('Xylem Inc.', 'Industrials'),
    'YUM': ('Yum! Brands', 'Consumer Discretionary'),
    'ZBRA': ('Zebra Technologies', 'Information Technology'),
    'ZBH': ('Zimmer Biomet', 'Health Care'),
    'ZTS': ('Zoetis', 'Health Care'),
    'CRM': ('Salesforce', 'Information Technology'),
    'HIG': ('Hartford Financial Services', 'Financials'),
    'HOOD': ('Robinhood Markets', 'Financials'),
    'IBKR': ('Interactive Brokers', 'Financials'),
    'LHX': ('L3Harris Technologies', 'Industrials'),
    'MA': ('Mastercard', 'Financials'),
    'MAR': ('Marriott International', 'Consumer Discretionary'),
    'MAS': ('Masco Corporation', 'Industrials'),
    'MKC': ('McCormick & Company', 'Consumer Staples'),
    'MRVL': ('Marvell Technology', 'Information Technology'),
    'MTB': ('M&T Bank', 'Financials'),
    'NCLH': ('Norwegian Cruise Line Holdings', 'Consumer Discretionary'),
    'NVR': ('NVR Inc.', 'Consumer Discretionary'),
    'PNC': ('PNC Financial', 'Financials'),
    'PODD': ('Insulet Corporation', 'Health Care'),
    'POOL': ('Pool Corporation', 'Consumer Discretionary'),
    'PSKY': ('Paramount Global', 'Communication Services'),
    'SLB': ('Schlumberger', 'Energy'),
    'SPGI': ('S&P Global', 'Financials'),
    'SWKS': ('Skyworks Solutions', 'Information Technology'),
    'TKO': ('TKO Group Holdings', 'Communication Services'),
    'TMO': ('Thermo Fisher Scientific', 'Health Care'),
    'TPR': ('Tapestry', 'Consumer Discretionary'),
    'TROW': ('T. Rowe Price', 'Financials'),
    'TTD': ('The Trade Desk', 'Information Technology'),
    'TTWO': ('Take-Two Interactive', 'Communication Services'),
    'EXPE': ('Expedia Group', 'Consumer Discretionary'),
    'FDS': ('FactSet', 'Financials'),
    'GPC': ('Genuine Parts Company', 'Consumer Discretionary'),
    'HES': ('Hess Corporation', 'Energy'),
    'LUV': ('Southwest Airlines', 'Industrials'),
    'MHK': ('Mohawk Industries', 'Consumer Discretionary'),
    'MLM': ('Martin Marietta Materials', 'Materials'),
    'MPC': ('Marathon Petroleum', 'Energy'),
    'NCLH': ('Norwegian Cruise Line Holdings', 'Consumer Discretionary'),
}

# ============================================================
# Nasdaq 100 구성종목 (2026-02 기준)
# S&P 500에 없는 종목만 추가 정의, 나머지는 SP500에서 참조
# ============================================================
NASDAQ100_EXTRA = {
    'ASML': ('ASML Holding', 'Information Technology'),
    'SHOP': ('Shopify', 'Information Technology'),
    'PDD': ('PDD Holdings', 'Consumer Discretionary'),
    'ARM': ('Arm Holdings', 'Information Technology'),
    'FER': ('Ferrovial SE', 'Industrials'),
    'MELI': ('MercadoLibre', 'Consumer Discretionary'),
    'MSTR': ('MicroStrategy', 'Information Technology'),
    'CCEP': ('Coca-Cola Europacific Partners', 'Consumer Staples'),
    'ALNY': ('Alnylam Pharmaceuticals', 'Health Care'),
    'INSM': ('Insmed Incorporated', 'Health Care'),
    'TRI': ('Thomson Reuters', 'Industrials'),
    'TEAM': ('Atlassian', 'Information Technology'),
    'ZS': ('Zscaler', 'Information Technology'),
}

# Nasdaq 100 종목 목록 (101개 심볼 - GOOGL/GOOG 별도)
NASDAQ100_TICKERS = [
    'NVDA', 'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'GOOG', 'META', 'AVGO', 'TSLA', 'WMT',
    'ASML', 'MU', 'COST', 'NFLX', 'AMD', 'PLTR', 'CSCO', 'LRCX', 'AMAT', 'TMUS',
    'LIN', 'INTC', 'PEP', 'AMGN', 'TXN', 'KLAC', 'GILD', 'ISRG', 'ADI', 'SHOP',
    'QCOM', 'HON', 'PDD', 'APP', 'BKNG', 'ARM', 'PANW', 'VRTX', 'CMCSA', 'SBUX',
    'INTU', 'ADBE', 'CEG', 'CRWD', 'MELI', 'WDC', 'MAR', 'STX', 'ADP', 'SNPS',
    'DASH', 'REGN', 'CDNS', 'MNST', 'ORLY', 'CTAS', 'MDLZ', 'CSX', 'ABNB', 'WBD',
    'AEP', 'MRVL', 'PCAR', 'ROST', 'BKR', 'FTNT', 'NXPI', 'MPWR', 'FER', 'FAST',
    'IDXX', 'EA', 'FANG', 'ADSK', 'XEL', 'EXC', 'CCEP', 'ALNY', 'MCHP', 'DDOG',
    'MSTR', 'KDP', 'ODFL', 'PYPL', 'TRI', 'GEHC', 'WDAY', 'TTWO', 'CPRT', 'ROP',
    'AXON', 'PAYX', 'INSM', 'CTSH', 'CHTR', 'KHC', 'DXCM', 'ZS', 'VRSK', 'TEAM',
    'CSGP',
]

# ============================================================
# Dow Jones 30 구성종목 (2026-02 기준)
# 모두 S&P 500에 포함됨
# ============================================================
DJIA_TICKERS = [
    'AAPL', 'AMGN', 'AMZN', 'AXP', 'BA', 'CAT', 'CRM', 'CSCO', 'CVX', 'DIS',
    'GS', 'HD', 'HON', 'IBM', 'JNJ', 'JPM', 'KO', 'MCD', 'MMM', 'MRK',
    'MSFT', 'NKE', 'NVDA', 'PG', 'SHW', 'TRV', 'UNH', 'V', 'VZ', 'WMT',
]


def get_all_tickers_info():
    """모든 인덱스의 종목 정보를 통합한 딕셔너리 반환"""
    combined = dict(SP500)
    combined.update(NASDAQ100_EXTRA)
    return combined


def get_all_unique_tickers():
    """세 인덱스의 모든 고유 티커 목록 반환"""
    all_tickers = set(SP500.keys())
    all_tickers.update(NASDAQ100_TICKERS)
    all_tickers.update(DJIA_TICKERS)
    return sorted(all_tickers)


def fetch_batch(tickers, retries=2):
    """yfinance 배치 다운로드"""
    for attempt in range(retries):
        try:
            data = yf.download(
                tickers,
                period="35d",
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
            )
            return data
        except Exception as e:
            print(f"  ⚠️ Batch attempt {attempt+1} failed: {e}")
            time.sleep(3)
    return None


def get_market_caps(tickers, max_workers=10):
    """시가총액 병렬 조회"""
    market_caps = {}

    def fetch_one(ticker):
        for _ in range(2):
            try:
                t = yf.Ticker(ticker)
                info = t.fast_info
                return ticker, getattr(info, 'market_cap', 0) or 0
            except Exception:
                time.sleep(1)
        return ticker, 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_one, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, mcap = future.result()
            market_caps[ticker] = mcap

    return market_caps


def process_stock(ticker, name, sector, price_data, market_cap):
    """개별 종목 데이터 가공"""
    try:
        closes = price_data['Close'].dropna()
        if len(closes) < 2:
            return None

        current_price = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else current_price

        # 1일 변동률
        change_1d = ((current_price - prev_close) / prev_close) * 100 if prev_close else 0

        # 5일 (1주) 변동률
        if len(closes) >= 6:
            price_5d_ago = float(closes.iloc[-6])
            change_5d = ((current_price - price_5d_ago) / price_5d_ago) * 100
        else:
            change_5d = change_1d

        # 22일 (1개월) 변동률
        if len(closes) >= 23:
            price_22d_ago = float(closes.iloc[-23])
            change_22d = ((current_price - price_22d_ago) / price_22d_ago) * 100
        else:
            change_22d = change_5d

        # 거래량
        volumes = price_data['Volume'].dropna()
        volume = int(volumes.iloc[-1]) if len(volumes) > 0 else 0

        return {
            "symbol": ticker,
            "name": name,
            "sector": sector,
            "current_price": round(current_price, 2),
            "market_cap": market_cap,
            "price_change_1d": round(change_1d, 2),
            "price_change_5d": round(change_5d, 2),
            "price_change_1mo": round(change_22d, 2),
            "total_volume": volume,
        }
    except Exception as e:
        print(f"  ⚠️ Error processing {ticker}: {e}")
        return None


def build_index_data(index_tickers, all_info, raw_data, market_caps, all_tickers_list):
    """특정 인덱스의 데이터를 빌드"""
    processed = []
    for ticker in index_tickers:
        if ticker not in all_info:
            continue
        name, sector = all_info[ticker]
        try:
            if len(all_tickers_list) > 1:
                price_data = raw_data[ticker]
            else:
                price_data = raw_data
            mcap = market_caps.get(ticker, 0)
            result = process_stock(ticker, name, sector, price_data, mcap)
            if result:
                processed.append(result)
        except Exception as e:
            print(f"  ⚠️ Skipping {ticker}: {e}")
    # 시총 순 정렬
    processed.sort(key=lambda x: x["market_cap"], reverse=True)
    return processed


def save_data(data, filename, label):
    """데이터를 JSON으로 저장"""
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"  💾 {label}: {len(data)}개 종목 → {filepath}")
    return filepath


def main():
    print(f"🚀 Fetching market data at {datetime.now(timezone.utc).isoformat()}")

    all_info = get_all_tickers_info()
    all_tickers = get_all_unique_tickers()

    print(f"  📊 총 고유 티커: {len(all_tickers)}개")
    print(f"     S&P 500: {len(SP500)}개")
    print(f"     Nasdaq 100: {len(NASDAQ100_TICKERS)}개")
    print(f"     Dow 30: {len(DJIA_TICKERS)}개")

    # 1) 가격 데이터 통합 배치 다운로드
    tickers_str = " ".join(all_tickers)
    print(f"  📊 Downloading price data for {len(all_tickers)} unique stocks...")
    raw_data = fetch_batch(tickers_str)
    if raw_data is None:
        print("  ❌ Failed to download price data")
        return

    # 2) 시가총액 통합 병렬 조회
    print("  💰 Fetching market caps...")
    market_caps = get_market_caps(all_tickers)

    # 3) 인덱스별 데이터 가공
    print("  🔄 Processing S&P 500...")
    sp500_data = build_index_data(list(SP500.keys()), all_info, raw_data, market_caps, all_tickers)

    print("  🔄 Processing Nasdaq 100 (QQQ)...")
    qqq_data = build_index_data(NASDAQ100_TICKERS, all_info, raw_data, market_caps, all_tickers)

    print("  🔄 Processing Dow 30 (DIA)...")
    dia_data = build_index_data(DJIA_TICKERS, all_info, raw_data, market_caps, all_tickers)

    # 4) 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    save_data(sp500_data, "sp500_heatmap.json", "S&P 500")
    save_data(qqq_data, "qqq_heatmap.json", "QQQ (Nasdaq 100)")
    save_data(dia_data, "dia_heatmap.json", "DIA (Dow 30)")

    # 통합 메타데이터
    meta = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "indexes": {
            "sp500": {"total_stocks": len(sp500_data), "top5": [s["symbol"] for s in sp500_data[:5]]},
            "qqq": {"total_stocks": len(qqq_data), "top5": [s["symbol"] for s in qqq_data[:5]]},
            "dia": {"total_stocks": len(dia_data), "top5": [s["symbol"] for s in dia_data[:5]]},
        }
    }
    meta_path = os.path.join(OUTPUT_DIR, "heatmap_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"  📋 Meta saved to {meta_path}")

    # 기존 호환성: sp500_heatmap_meta.json 도 유지
    sp500_meta = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_stocks": len(sp500_data),
        "stocks": sp500_data,
    }
    with open(os.path.join(OUTPUT_DIR, "sp500_heatmap_meta.json"), "w", encoding="utf-8") as f:
        json.dump(sp500_meta, f, ensure_ascii=False, indent=2)

    print(f"\n  ✅ 완료!")
    print(f"     S&P 500: {len(sp500_data)}개 | QQQ: {len(qqq_data)}개 | DIA: {len(dia_data)}개")


if __name__ == "__main__":
    main()
