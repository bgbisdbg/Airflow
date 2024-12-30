-- создаём схему
create schema if not exists "DS";

-- создаём таблицы
-- остатки средств на счетах
create table if not exists
	"DS".ft_balance_f (
		on_date date not null,
		account_rk integer not null,
		currency_rk integer,
		balance_out decimal,
		PRIMARY KEY(on_date, account_rk)
);


-- Проводки (движения средств) по счетам
CREATE TABLE IF NOT EXISTS "DS".ft_posting_f (
    oper_date DATE NOT NULL,
    credit_account_rk INTEGER NOT NULL,
    debet_account_rk INTEGER NOT NULL,
    credit_amount DECIMAL,
    debet_amount DECIMAL
);



-- информация о счетах клиентов
create table if not exists
	"DS".md_account_d (
		data_actual_date date not null,
		data_actual_end_date date not null,
		account_rk integer not null,
		account_number varchar(20) not null,
		char_type varchar(1) not null,
		currency_rk integer not null,
		currency_code varchar(3) not null,
		PRIMARY KEY(data_actual_date, account_rk)
);

-- справочник валют
create table if not exists
	"DS".md_currency_d (
		currency_rk integer not null,
		data_actual_date date not null,
		data_actual_end_date date,
		currency_code varchar(25),
		code_iso_char varchar(3),
		PRIMARY KEY(currency_rk, data_actual_date)
);

-- курсы валют
create table if not exists
	"DS".md_exchange_rate_d (
		data_actual_date date not null,
		data_actual_end_date date,
		currency_rk integer not null,
		reduced_cource decimal,
		code_iso_num varchar(3),
		PRIMARY KEY(data_actual_date, currency_rk)
);

-- справочник балансовых счётов
CREATE TABLE IF NOT EXISTS "DS".md_ledger_account_s (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    start_date DATE NOT NULL,
    end_date DATE,
    PRIMARY KEY (ledger_account, start_date)
);