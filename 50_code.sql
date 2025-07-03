CREATE OR REPLACE FUNCTION bid_winner_set(a_id INTEGER) RETURNS VOID LANGUAGE sql AS
$_$
-- a_id: ID тендера
-- функция рассчитывает победителей заданного тендера
-- и заполняет поля bid.is_winner и bid.win_amount

  
WITH ranked_bids AS (
    SELECT 
        b.id as bid_id,
        b.tender_id,
        b.product_id,
        b.amount,
        b.price,
        tp.amount as total_amount,
        tp.start_price,
        tp.bid_step,
        ROW_NUMBER() OVER (PARTITION BY b.tender_id, b.product_id ORDER BY b.price DESC, b.id ASC) as bid_rank -- Добавляем номер строки в каждом окне

    FROM bid as b
    JOIN tender_product tp ON b.tender_id = tp.id AND b.product_id = tp.product_id
    WHERE b.tender_id = a_id
),
cumulative_amounts AS (
    SELECT 
        rb.*,
        SUM(rb.amount) OVER (
            PARTITION BY rb.tender_id, rb.product_id 
            ORDER BY rb.price DESC, rb.bid_id ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as running_total --Добавляем увеличивающуюся сумму в каждом окне
    FROM ranked_bids rb
),
winning_calculations AS (
    SELECT 
        ca.*,
        CASE 
            WHEN ca.running_total <= ca.total_amount THEN ca.amount --Все забрал
            WHEN ca.running_total - ca.amount < ca.total_amount THEN ca.total_amount - (ca.running_total - ca.amount) --Забрал часть
            ELSE 0
        END as calculated_win_amount
    FROM cumulative_amounts ca
),
winning_update AS (
    SELECT 
        t.*,
        CASE WHEN (t.calculated_win_amount > 0 ) THEN true ELSE false END as is_winner,
        CASE WHEN (t.calculated_win_amount = 0 or t.calculated_win_amount = t.amount) THEN NULL ELSE t.calculated_win_amount END as win_amount --Не пон
ятно зачем там NULL если он выйграл
        --CASE WHEN (t.calculated_win_amount = 0 ) THEN NULL ELSE t.calculated_win_amount END as win_amount --Правильный объем который выйграл
    FROM winning_calculations as t
)
--select * from winning_update

UPDATE bid as b
SET 
    is_winner = ar.is_winner,
    win_amount = ar.win_amount
FROM winning_update as ar
WHERE b.id = ar.bid_id 
AND b.tender_id = ar.tender_id 
AND b.product_id = ar.product_id;

-- ...
;
$_$;
