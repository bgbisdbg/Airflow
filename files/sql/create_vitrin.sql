-- Создание схемы "DM" (если она не существует)
CREATE SCHEMA IF NOT EXISTS "DM";

-- Создание таблицы "DM".dm_account_turnover_f
CREATE TABLE IF NOT EXISTS "DM".dm_account_turnover_f (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8),
    CONSTRAINT pk_dm_account_turnover_f PRIMARY KEY (on_date, account_rk)
);

-- Создание таблицы "DM".dm_account_balance_f
CREATE TABLE IF NOT EXISTS "DM".dm_account_balance_f (
    on_date DATE NOT NULL,
    account_rk NUMERIC NOT NULL,
    balance_out NUMERIC(23, 8),
    balance_out_rub NUMERIC(23, 8),
    CONSTRAINT pk_dm_account_balance_f PRIMARY KEY (on_date, account_rk)
);

-- Функция "DM".fill_account_turnover_f
CREATE OR REPLACE FUNCTION "DM".fill_account_turnover_f(i_ondate DATE)
RETURNS VOID AS
$$
BEGIN
    DELETE FROM "DM".dm_account_turnover_f WHERE on_date = i_OnDate;

    INSERT INTO "DM".dm_account_turnover_f (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    SELECT
        i_ondate AS on_date,
        mad.account_rk,
        COALESCE(SUM(fpf.credit_amount), 0) AS credit_amount,
        COALESCE(SUM(fpf.credit_amount * COALESCE(merd.reduced_cource, 1)), 0) AS credit_amount_rub,
        COALESCE(SUM(fpf.debet_amount), 0) AS debet_amount,
        COALESCE(SUM(fpf.debet_amount * COALESCE(merd.reduced_cource, 1)), 0) AS debet_amount_rub
    FROM
        "DS".md_account_d mad
    LEFT JOIN
        "DS".ft_posting_f fpf
        ON mad.account_rk = fpf.credit_account_rk OR mad.account_rk = fpf.debet_account_rk
    LEFT JOIN
        "DS".md_exchange_rate_d merd
        ON mad.currency_rk = merd.currency_rk
        AND merd.data_actual_date <= i_ondate
        AND (merd.data_actual_end_date IS NULL OR merd.data_actual_end_date >= i_ondate)
    WHERE
        fpf.oper_date = i_ondate
    GROUP BY
        mad.account_rk;
END;
$$
LANGUAGE plpgsql;

-- Функция "DM".fill_account_balance_f
CREATE OR REPLACE FUNCTION "DM".fill_account_balance_f(i_ondate DATE)
RETURNS VOID AS
$$
BEGIN
    DELETE FROM "DM".dm_account_balance_f WHERE on_date = i_ondate;

    INSERT INTO "DM".dm_account_balance_f (
        on_date,
        account_rk,
        balance_out,
        balance_out_rub
    )
    SELECT
        i_ondate AS on_date,
        mad.account_rk,
        CASE
            WHEN mad.char_type = 'А' THEN
                COALESCE((SELECT balance_out FROM "DM".dm_account_balance_f WHERE account_rk = mad.account_rk AND on_date = i_ondate - INTERVAL '1 day'), 0)
                + COALESCE((SELECT debet_amount FROM "DM".dm_account_turnover_f WHERE account_rk = mad.account_rk AND on_date = i_ondate), 0)
                - COALESCE((SELECT credit_amount FROM "DM".dm_account_turnover_f WHERE account_rk = mad.account_rk AND on_date = i_ondate), 0)
            WHEN mad.char_type = 'П' THEN
                COALESCE((SELECT balance_out FROM "DM".dm_account_balance_f WHERE account_rk = mad.account_rk AND on_date = i_ondate - INTERVAL '1 day'), 0)
                - COALESCE((SELECT debet_amount FROM "DM".dm_account_turnover_f WHERE account_rk = mad.account_rk AND on_date = i_ondate), 0)
                + COALESCE((SELECT credit_amount FROM "DM".dm_account_turnover_f WHERE account_rk = mad.account_rk AND on_date = i_ondate), 0)
        END AS balance_out,
        CASE
            WHEN mad.char_type = 'А' THEN
                COALESCE((SELECT balance_out_rub FROM "DM".dm_account_balance_f WHERE account_rk = mad.account_rk AND on_date = i_ondate - INTERVAL '1 day'), 0)
                + COALESCE((SELECT debet_amount_rub FROM "DM".dm_account_turnover_f WHERE account_rk = mad.account_rk AND on_date = i_ondate), 0)
                - COALESCE((SELECT credit_amount_rub FROM "DM".dm_account_turnover_f WHERE account_rk = mad.account_rk AND on_date = i_ondate), 0)
            WHEN mad.char_type = 'П' THEN
                COALESCE((SELECT balance_out_rub FROM "DM".dm_account_balance_f WHERE account_rk = mad.account_rk AND on_date = i_ondate - INTERVAL '1 day'), 0)
                - COALESCE((SELECT debet_amount_rub FROM "DM".dm_account_turnover_f WHERE account_rk = mad.account_rk AND on_date = i_ondate), 0)
                + COALESCE((SELECT credit_amount_rub FROM "DM".dm_account_turnover_f WHERE account_rk = mad.account_rk AND on_date = i_ondate), 0)
        END AS balance_out_rub
    FROM
        "DS".md_account_d mad
    WHERE
        mad.data_actual_date <= i_ondate
        AND (mad.data_actual_end_date IS NULL OR mad.data_actual_end_date >= i_ondate);
END;
$$
LANGUAGE plpgsql;

-- Заполнение таблицы "DM".dm_account_balance_f данными за 2017-12-31
INSERT INTO "DM".dm_account_balance_f (
    on_date,
    account_rk,
    balance_out,
    balance_out_rub
)
SELECT
    '2017-12-31'::DATE AS on_date,
    fbf.account_rk,
    fbf.balance_out,
    fbf.balance_out * COALESCE(merd.reduced_cource, 1) AS balance_out_rub
FROM
    "DS".ft_balance_f fbf
LEFT JOIN
    "DS".md_exchange_rate_d merd
    ON fbf.currency_rk = merd.currency_rk
    AND merd.data_actual_date <= '2017-12-31'
    AND (merd.data_actual_end_date IS NULL OR merd.data_actual_end_date >= '2017-12-31')
ON CONFLICT (on_date, account_rk) DO UPDATE
SET
    balance_out = EXCLUDED.balance_out,
    balance_out_rub = EXCLUDED.balance_out_rub;