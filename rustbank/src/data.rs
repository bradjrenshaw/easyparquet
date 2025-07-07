use fake::faker::chrono::ar_sa::DateTimeBetween;
use mysql_async::prelude::FromRow;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use fake::{Dummy, Fake};
use fake::faker::name::en::{FirstName, LastName};
use fake::faker::phone_number::en::PhoneNumber;
use fake::faker::internet::en::SafeEmail;
use fake::faker::address::en::StreetName;
use fake::faker::chrono::en::{DateTime};
use fake::faker::creditcard::en::CreditCardNumber;
use fake::faker::currency::en::CurrencyCode;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use chrono::TimeZone;

pub trait Data {
    fn generate(rows: usize) -> Self;
    fn table_name() -> &'static str;
    fn create_query() -> String;
    fn insert_query() -> String;
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow, Serialize, Deserialize)]
pub struct User {
    first_name: String,
    last_name: String,
    email: String,
    address: String,
    phone_number: String,
    date_of_birth: NaiveDate,
    created_at: NaiveDateTime,
}

impl Data for User {

    fn generate(_rows: usize) -> Self {
        //Fake implemented manually here as I had significant issues with the dummy implementation
            let start_date: DateTime<Utc> = Utc.with_ymd_and_hms(1000, 1, 1, 0, 0, 0).unwrap();
            let end_date: DateTime<Utc> = Utc.with_ymd_and_hms(9999, 1, 1, 0, 0, 0).unwrap();
            let date_faker = DateTimeBetween(start_date, end_date);
        User {
            first_name: FirstName().fake(),
            last_name: LastName().fake(),
            email: SafeEmail().fake(),
            address: StreetName().fake(),
            phone_number: PhoneNumber().fake(),
            date_of_birth: date_faker.fake::<DateTime<Utc>>().naive_utc().date(),
            created_at: date_faker.fake::<DateTime<Utc>>().naive_utc()                                                            
        }
    }

    fn table_name() -> &'static str {
        "users"
    }

    fn create_query() -> String {
        format!(
        r"CREATE TABLE IF NOT EXISTS {} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        first_name VARCHAR(255) NOT NULL,
        last_name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        address VARCHAR(255) NOT NULL,
        phone_number VARCHAR(50) NOT NULL,
        date_of_birth DATE NOT NULL,
        created_at DATETIME NOT NULL
        );",
        Self::table_name()
    )
    }

    fn insert_query() -> String {
        format!(r"INSERT INTO {} (first_name, last_name, email, address, phone_number, date_of_birth, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)",
    Self::table_name()
    )
    }
}

impl Into<mysql_async::Params> for User {

    fn into(self) -> mysql_async::Params {
        mysql_async::Params::from((
            self.first_name,
            self.last_name,
            self.email,
            self.address,
            self.phone_number,
            self.date_of_birth,
            self.created_at
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, FromRow, Dummy, Serialize, Deserialize)]
pub struct Account {

    #[dummy(faker = "1..=100")]
    user_id: u64,

    #[dummy(faker="CreditCardNumber()")]
    account_number: String,

    #[dummy(faker = "CurrencyCode()")]
    currency_code: String,

    balance: Decimal,

    #[dummy(faker = "DateTime()")]
    created_at: NaiveDateTime,
}

impl Data for Account {

    fn generate(rows: usize) -> Self {
        let random_int: i128 = (50..=1000000).fake();
        let balance_decimal = Decimal::from_i128_with_scale(random_int, 2);
                    let start_date: DateTime<Utc> = Utc.with_ymd_and_hms(1000, 1, 1, 0, 0, 0).unwrap();
            let end_date: DateTime<Utc> = Utc.with_ymd_and_hms(9999, 1, 1, 0, 0, 0).unwrap();
            let date_faker = DateTimeBetween(start_date, end_date);
        Self {
            user_id: (1..=rows as u64).fake(),
            account_number: CreditCardNumber().fake(),
            currency_code: CurrencyCode().fake(),
            balance: balance_decimal,
                        created_at: date_faker.fake::<DateTime<Utc>>().naive_utc()                                                            
        }
    }

    fn table_name() -> &'static str {
        "accounts"
    }

    fn create_query() -> String {
        format!(r"CREATE TABLE IF NOT EXISTS {} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        user_id BIGINT UNSIGNED NOT NULL,
        account_number VARCHAR(255) NOT NULL,
        currency_code VARCHAR(3) NOT NULL,
        balance DECIMAL(19, 2) NOT NULL,
        created_at DATETIME NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );", Self::table_name())
    }

    fn insert_query() -> String {
        format!(r"INSERT INTO {} (user_id, account_number, currency_code, balance, created_at)
        VALUES (?, ?, ?, ?, ?)",
    Self::table_name()
    )
    }
}

impl Into<mysql_async::Params> for Account {

        fn into(self) -> mysql_async::Params {
            mysql_async::Params::from((
                self.user_id,
                self.account_number,
                self.currency_code,
                self.balance,
                self.created_at
            ))
        }
}
