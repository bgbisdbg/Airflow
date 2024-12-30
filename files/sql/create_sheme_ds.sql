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
create table if not exists
	"DS".md_ledger_account_s (
		chapter char(1),
		chapter_name varchar(16),
		section_number integer,
		section_name varchar(22),
		subsection_name varchar(21),
		ledger1_account integer,
		ledger1_account_name varchar(47),
		ledger_account integer not null,
		ledger_account_name varchar(153),
		characteristic char(1),
		is_resident integer,
		is_reserve integer,
		is_reserved integer,
		is_loan integer,
		is_reserved_assets integer,
		is_overdue integer,
		is_interest integer,
		pair_account varchar(5),
		start_date date not null,
		end_date date,
		is_rub_only integer,
		min_term varchar(1),
		min_term_measure varchar(1),
		max_term varchar(1),
		max_term_measure varchar(1),
		ledger_acc_full_name_translit varchar(1),
		is_revaluation varchar(1),
		is_correct varchar(1),
		PRIMARY KEY(ledger_account, start_date)
);