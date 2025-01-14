--Скрипт на создание витрины под 101 форму
CREATE TABLE IF NOT EXISTS "DM".DM_F101_ROUND_F (
    FROM_DATE DATE NOT NULL,
    TO_DATE DATE NOT NULL,
    CHAPTER CHAR(1) NOT NULL,
    LEDGER_ACCOUNT CHAR(5) NOT NULL,
    CHARACTERISTIC CHAR(1) NOT NULL,
    BALANCE_IN_RUB NUMERIC(23,8),
    BALANCE_IN_VAL NUMERIC(23,8),
    BALANCE_IN_TOTAL NUMERIC(23,8),
    TURN_DEB_RUB NUMERIC(23,8),
    TURN_DEB_VAL NUMERIC(23,8),
    TURN_DEB_TOTAL NUMERIC(23,8),
    TURN_CRE_RUB NUMERIC(23,8),
    TURN_CRE_VAL NUMERIC(23,8),
    TURN_CRE_TOTAL NUMERIC(23,8),
    BALANCE_OUT_RUB NUMERIC(23,8),
    BALANCE_OUT_VAL NUMERIC(23,8),
    BALANCE_OUT_TOTAL NUMERIC(23,8)
);

CREATE TABLE IF NOT EXISTS "DS".DM_F101_ROUND_F_V2 (
    FROM_DATE DATE NOT NULL,
    TO_DATE DATE NOT NULL,
    CHAPTER CHAR(1) NOT NULL,
    LEDGER_ACCOUNT CHAR(5) NOT NULL,
    CHARACTERISTIC CHAR(1) NOT NULL,
    BALANCE_IN_RUB NUMERIC(23,8),
    BALANCE_IN_VAL NUMERIC(23,8),
    BALANCE_IN_TOTAL NUMERIC(23,8),
    TURN_DEB_RUB NUMERIC(23,8),
    TURN_DEB_VAL NUMERIC(23,8),
    TURN_DEB_TOTAL NUMERIC(23,8),
    TURN_CRE_RUB NUMERIC(23,8),
    TURN_CRE_VAL NUMERIC(23,8),
    TURN_CRE_TOTAL NUMERIC(23,8),
    BALANCE_OUT_RUB NUMERIC(23,8),
    BALANCE_OUT_VAL NUMERIC(23,8),
    BALANCE_OUT_TOTAL NUMERIC(23,8),
    PRIMARY KEY (LEDGER_ACCOUNT, FROM_DATE, TO_DATE)
);

--Фунеция для содазние 101 формы
CREATE OR REPLACE PROCEDURE "DM".fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_FromDate DATE;
    v_ToDate DATE;
BEGIN
    DELETE FROM "DM".DM_F101_ROUND_F WHERE FROM_DATE = i_ondate;

    v_FromDate := DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 MONTH';
    v_ToDate := DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 DAY';

    INSERT INTO "DM".DM_F101_ROUND_F (
        FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
        BALANCE_IN_RUB, BALANCE_IN_VAL, BALANCE_IN_TOTAL,
        TURN_DEB_RUB, TURN_DEB_VAL, TURN_DEB_TOTAL,
        TURN_CRE_RUB, TURN_CRE_VAL, TURN_CRE_TOTAL,
        BALANCE_OUT_RUB, BALANCE_OUT_VAL, BALANCE_OUT_TOTAL
    )
    SELECT
        v_FromDate AS FROM_DATE,
        v_ToDate AS TO_DATE,
        mlas.chapter AS CHAPTER,
        LEFT(mad.account_number, 5) AS LEDGER_ACCOUNT,
        mad.char_type AS CHARACTERISTIC,
        SUM(dabf.balance_out_rub) AS BALANCE_IN_RUB,
        SUM(dabf.balance_out) AS BALANCE_IN_VAL,
        SUM(dabf.balance_out + dabf.balance_out_rub) AS BALANCE_IN_TOTAL,
        SUM(datf.debet_amount_rub) AS TURN_DEB_RUB,
        SUM(datf.debet_amount) AS TURN_DEB_VAL,
        SUM(datf.debet_amount + datf.debet_amount_rub) AS TURN_DEB_TOTAL,
        SUM(datf.credit_amount_rub) AS TURN_CRE_RUB,
        SUM(datf.credit_amount) AS TURN_CRE_VAL,
        SUM(datf.credit_amount + datf.credit_amount_rub) AS TURN_CRE_TOTAL,
        SUM(dabf_end.balance_out_rub) AS BALANCE_OUT_RUB,
        SUM(dabf_end.balance_out) AS BALANCE_OUT_VAL,
        SUM(dabf_end.balance_out + dabf_end.balance_out_rub) AS BALANCE_OUT_TOTAL
    FROM
        "DM".dm_account_balance_f dabf
        JOIN "DS".md_account_d mad ON dabf.account_rk = mad.account_rk
        JOIN "DS".md_ledger_account_s mlas ON mlas.ledger_account::VARCHAR = LEFT(mad.account_number, 5)
        JOIN "DM".dm_account_turnover_f datf ON mad.account_rk = datf.account_rk
        LEFT JOIN "DM".dm_account_balance_f dabf_end ON mad.account_rk = dabf_end.account_rk AND dabf_end.on_date = v_ToDate
    WHERE
        dabf.on_date = v_FromDate - INTERVAL '1 DAY'
    GROUP BY
        mlas.chapter, LEFT(mad.account_number, 5), mad.char_type;
END;
$$;