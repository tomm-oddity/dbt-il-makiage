with

source as (
    select * from {{ source('magento', 'orders') }}
),

renamed as (

    select
        entity_id,
        increment_id,
        created_at,
        updated_at,
        status,
        grand_total,
        shipping_amount,
        shipping_address_id,
        billing_address_id,
        store_id,
        order_type,
        {{ hash_column('customer_email') }} as email_hash,
        (
            {{ is_test_email('customer_email') }}
        ) as is_test_email,
        (
            lower(order_type) like '%prepaid plan%'
            or replace(lower(status), '_', ' ') like '%prepaid plan%'
            or replace(lower(status), ' ', '_') like '%canceled%'
            or replace(lower(status), ' ', '_') like '%not% real order%'
            or lower(increment_id) like '%-b%'
            or lower(increment_id) like '%-g_'
        ) as is_fake_order,
        (
            is_test_email

            -- employee coupons
            or coupon_code like 'HAPPYHOLIDAY%'
            or upper(coupon_code) like '%EMPLOYEE2019%'

            -- partners and influencers coupon codes
            or lower(customer_email) like '%@example.com%'
            or lower(customer_email) like '%@halpern.co.uk%'
            or lower(customer_email) like '%@arsenal.co.uk%'
            or lower(customer_email) like '%@fakepr.de%'
            or upper(coupon_code) like 'YBPRUN%'
            or upper(coupon_code) like 'INF%'
            or upper(coupon_code) like 'MKTG%'
            or upper(coupon_code) like 'CONT%'
            or upper(coupon_code) like '150CS%'
            or upper(coupon_code) like 'TPPRUN%'
            or upper(coupon_code) like '%BOOZIKA%'
            or upper(coupon_code) like 'JRFG%'
            or upper(coupon_code) like '%LOOKS%'

            -- abused coupons (were used a lot of times by the same customer)
            or upper(coupon_code) like 'UK40SR87ZR0RO16%'
            or upper(coupon_code) like 'GSGEZJ-7LC-PPP%'
            or upper(coupon_code) like '%CS20AL1-RB7-EW4%'
            or upper(coupon_code) like '%CA50397-3SV-XSQ%'
            or upper(coupon_code) like '%CA50Z68-DO1-CK0%'
            or upper(coupon_code) like '%CA50OT3-X5I-W5R%'
            or upper(coupon_code) like '%PA50A0J-H7K-1V6%'
            or upper(coupon_code) like '%PA50EMC-1AI-5LS%'
            or upper(coupon_code) like '%PA50EAZ-OGS-1KD%'
            or upper(coupon_code) like '%CS50A2G-VZW-I4O%'
            or upper(coupon_code) like '%CS20I5S-FIK-2LJ%'
            or upper(coupon_code) like '%120-DJ69-EFYF%'
            or upper(coupon_code) like '%PA50CWH-CMT-NT0%'
            or upper(coupon_code) like '%CS20F3V-VZF-JKF%'
            or upper(coupon_code) like '%GSGSTB-81K-YAF%'
            or upper(coupon_code) like '%GSG1Y6-IKZ-529%'
            or upper(coupon_code) like '%PA508SU-VXW-Z9G%'
            or upper(coupon_code) like '%ACHIAD99%'
            or upper(coupon_code) like '%CS50841-5KJ-KVV%'
            or upper(coupon_code) like '%CS20N95-7UU-1QL%'
            or upper(coupon_code) like '%PA50ETH-XGP-91M%'
            or upper(coupon_code) like '%UK40SRBAIV33NM4%'

            -- fraudulent customers
            or lower(customer_email) like '%animat995@gmail.com%'
            or lower(customer_email) like '%ow114349@gmail.com%'
            or lower(customer_email) like '%rl864861@gmail.com%'
            or lower(customer_email) like '%jeromethomas31300@gmail.com%'
            or lower(customer_email) like '%p.ealing007@gmail.com%'
            or lower(customer_email) like '%rscecom@gmail.com%'
            or lower(customer_email) like '%angelicareyesnambawan@gmail.com%'
            or lower(customer_email) like '%dj0887538@gmail.com%'
        ) as is_non_customer_order
    from
        source
)

select * from renamed
