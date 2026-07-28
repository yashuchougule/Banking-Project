import csv
import json
import random
from datetime import datetime, timedelta

# COnfig

N = 10_000_000


cities = ["Mumbai", "Pune", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Ahmedabad", "Kolkata", "Surat", "Jaipur", "Noida"]
states = {"Mumbai":"MH","Pune":"MH","Delhi":"DL","Bangalore":"KA","Chennai":"TN", "Hyderabad":"TS", "Ahmedabad":"GJ","Kolkata":"WB","Surat":"GJ","Jaipur":"RJ", "Noida":"UP"}

first_names = ["Aarav","Vivaan","Aditya","Arjun","Rohan","Rahul","Priya","Neha","Kavya","Isha", "Yash", "Ananya", "Saanvi", "Diya", "Aanya", "Myra", "Ishaan", "Kabir", "Shaurya", "Reyansh", "Anika", "Siddharth", "Aarohi", "Devansh", "Kiara"]
last_names = ["Sharma","Verma","Singh","Patel","Gupta","Yadav","Mehta","Reddy", "Chougule", "Joshi", "Nair", "Iyer", "Desai", "Kumar", "Das", "Chatterjee", "Ghosh", "Sarkar", "Bose", "Roy", "Sen", "Mishra", "Trivedi", "Shah", "Kapoor"]


# SPEED OPTIMIZATION 

r = random.randint
c = random.choice

# Helper functions

def rand_date():
    start = datetime(2015, 1, 1)
    end = datetime(2025, 12, 31)
    return start + timedelta(days=r(0, (end-start).days))

def phone():
    return str(r(6000000000, 9999999999))

def account():
    return str(r(100000000000, 999999999999))

def name():
    return c(first_names) + " " + c(last_names)

def city_state():
    city = c(cities)
    return city, states[city]

def risk(score):
    if score >= 750:
        return "LOW"
    elif score >= 600:
        return "MEDIUM"
    return "HIGH"

# 1 CORE BANKING

def generate_core_banking():
    with open("core_banking.csv","w",newline="",buffering=1024*1024) as f:
        w = csv.writer(f)
        w.writerow([
            "customer_id","full_name","gender","dob","age",
            "phone","email","address","city","state",
            "pincode","kyc_status","kyc_date",
            "account_no","account_type","account_open_date",
            "account_status","nominee_name","nominee_relation",
            "occupation","annual_income","credit_score"
        ])

        for i in range(N):
            city, state = city_state()

            w.writerow([
                f"CUST{i}", name(),
                c(["M","F"]),
                rand_date().date(),
                r(21,70),
                phone(),
                f"user{i}@mail.com",
                f"{r(1,200)} MG Road",
                city, state,
                r(100000,999999),
                c(["Verified","Pending"]),
                rand_date().date(),
                account(),
                c(["Savings","Current"]),
                rand_date().date(),
                c(["Active","Inactive"]),
                name(),
                c(["Spouse","Parent","Sibling"]),
                c(["Salaried","Self-Employed"]),
                r(200000,2000000),
                r(300,900)
            ])

# 2 ATM / POS

def generate_atm_pos():
    with open("atm_pos.csv","w",newline="",buffering=1024*1024) as f:
        w = csv.writer(f)
        w.writerow([
            "transaction_id","customer_id","transaction_type",
            "amount","transaction_date","transaction_time",
            "merchant_name","merchant_category",
            "atm_id","terminal_id",
            "city","state","response_code",
            "available_balance","device_type"
        ])

        for i in range(N*2):
            city, state = city_state()

            w.writerow([
                f"TXN{i}",
                f"CUST{r(0,N-1)}",
                c(["ATM_WITHDRAWAL","POS_PURCHASE"]),
                r(100,50000),
                rand_date().date(),
                f"{r(0,23)}:{r(0,59)}",
                c(["Amazon","Flipkart","Reliance"]),
                c(["Grocery","Shopping","Fuel"]),
                f"ATM{r(1000,9999)}",
                f"TERM{r(10000,99999)}",
                city, state,
                c(["SUCCESS","FAILED"]),
                r(0,200000),
                c(["ATM","POS"])
            ])

# 3 DIGITAL LOGS

def generate_digital_logs():
    with open("digital_logs.csv","w",newline="",buffering=1024*1024) as f:
        w = csv.writer(f)
        w.writerow([
            "log_id","customer_id","session_id",
            "login_time","logout_time",
            "ip_address","device_type","os",
            "browser","action_type","status",
            "city","failure_reason"
        ])

        for i in range(N*2):
            w.writerow([
                f"LOG{i}",
                f"CUST{r(0,N-1)}",
                f"SESS{r(1000,9999)}",
                rand_date(),
                rand_date(),
                f"192.168.{r(1,255)}.{r(1,255)}",
                c(["Mobile","Web"]),
                c(["Android","iOS","Windows"]),
                c(["Chrome","Safari"]),
                c(["LOGIN","TRANSFER","PAYMENT"]),
                c(["SUCCESS","FAILED"]),
                c(cities),
                c(["None","OTP Error","Timeout"])
            ])

# 4 CREDIT CARD

def generate_credit_card():
    with open("credit_card.csv","w",newline="",buffering=1024*1024) as f:
        w = csv.writer(f)
        w.writerow([
            "card_number","customer_id","card_type",
            "credit_limit","available_limit",
            "outstanding_balance","minimum_due",
            "billing_start","billing_end",
            "due_date","interest_rate",
            "card_status","reward_points"
        ])

        for i in range(N):
            limit = r(50000,500000)

            w.writerow([
                account(),
                f"CUST{i}",
                c(["GOLD","SILVER","PLATINUM"]),
                limit,
                r(0,limit),
                r(0,limit),
                r(1000,5000),
                rand_date().date(),
                rand_date().date(),
                rand_date().date(),
                round(random.uniform(12,36),2),
                c(["ACTIVE","BLOCKED"]),
                r(0,50000)
            ])

# 5 LOANS

def generate_loans():
    with open("loans.csv","w",newline="",buffering=1024*1024) as f:
        w = csv.writer(f)
        w.writerow([
            "loan_id","customer_id","loan_type",
            "loan_amount","interest_rate","loan_tenure_months",
            "emi_amount","disbursement_date","loan_status",
            "outstanding_amount","next_due_date",
            "payment_frequency","collateral_flag",
            "credit_score_at_approval","risk_category"
        ])

        for i in range(N):
            amt = r(100000,2000000)
            rate = r(7,18)
            tenure = r(12,240)
            score = r(300,900)

            emi = round(amt * rate / 1200,2)

            w.writerow([
                f"LOAN{i}",
                f"CUST{i}",
                c(["HOME","CAR","PERSONAL","EDUCATION"]),
                amt,
                rate,
                tenure,
                emi,
                rand_date().date(),
                c(["ACTIVE","CLOSED","DEFAULTED"]),
                r(0,amt),
                rand_date().date(),
                "MONTHLY",
                c(["YES","NO"]),
                score,
                risk(score)
            ])

# 6 KYC

def generate_kyc():
    with open("kyc.csv","w",newline="",buffering=1024*1024) as f:
        w = csv.writer(f)
        w.writerow([
            "kyc_id","customer_id","document_type",
            "document_number","issue_date","expiry_date",
            "verification_status","verification_method",
            "uploaded_date","address_match","name_match"
        ])

        for i in range(N):
            w.writerow([
                f"KYC{i}",
                f"CUST{i}",
                c(["AADHAR","PAN","PASSPORT"]),
                r(1000000000,9999999999),
                rand_date().date(),
                rand_date().date(),
                c(["VERIFIED","PENDING","REJECTED"]),
                c(["OCR","MANUAL"]),
                rand_date().date(),
                c(["YES","NO"]),
                c(["YES","NO"])
            ])

# 7 MARKET JSON

def generate_market():
    data = {
        "request_id": f"REQ{r(1000,9999)}",
        "timestamp": str(rand_date()),
        "stocks": {
            "TCS": r(3000,4000),
            "INFY": r(1200,2000),
            "HDFC": r(1500,3000)
        },
        "interest_rates": [
            {"country":"India","rate":round(random.uniform(5,8),2)},
            {"country":"USA","rate":round(random.uniform(4,7),2)}
        ]
    }

    with open("market.json","w") as f:
        json.dump(data,f,indent=4)

# 8 FRAUD STREAM

def generate_fraud():
    with open("fraud.json","w",buffering=1024*1024) as f:
        for i in range(N*3):
            event = {
                "event_id": f"EVT{i}",
                "customer_id": f"CUST{r(0,N-1)}",
                "transaction_id": f"TXN{i}",
                "amount": r(100,50000),
                "device_id": f"DEV{r(1000,9999)}",
                "ip": f"10.0.{r(1,255)}.{r(1,255)}",
                "fraud_score": r(0,100),
                "timestamp": str(rand_date())
            }
            f.write(json.dumps(event) + "\n")

# RUN ALL

if __name__ == "__main__":
    generate_core_banking()
    generate_atm_pos()
    generate_digital_logs()
    generate_credit_card()
    generate_loans()
    generate_kyc()
    generate_market()
    generate_fraud()

print("ALL DATASETS GENERATED SUCCESSFULLY (10M READY)")