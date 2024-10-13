
{% set doctorset = source('Data-warehouse', 'doctorset_raw') %}
{% set eahci = source('Data-warehouse', 'eahci_raw') %}
{% set yetenaweg = source('Data-warehouse', 'yetenaweg_raw') %}
{% set lobelia4cosmetics = source('Data-warehouse', 'lobelia4_raw') %}

with doctorset as (
    select * from {{ doctorset }}
),
eahci as (
    select * from {{ eahci }}
),
yetenaweg as (
    select * from {{ yetenaweg }}
),
lobelia4cosmetics as (
    select * from {{ lobelia4cosmetics }}
)

select
    doctorset.id,
    doctorset.image_id,
    doctorset.x_min,
    doctorset.y_min,
    doctorset.x_max,
    doctorset.y_max,
    doctorset.confidence,
    doctorset.class_id,
    doctorset.class_name,
    doctorset.timestamp
from doctorset

union all

select
    eahci.id,
    eahci.image_id,
    eahci.x_min,
    eahci.y_min,
    eahci.x_max,
    eahci.y_max,
    eahci.confidence,
    eahci.class_id,
    eahci.class_name,
    eahci.timestamp
from eahci

union all

select
    yetenaweg.id,
    yetenaweg.image_id,
    yetenaweg.x_min,
    yetenaweg.y_min,
    yetenaweg.x_max,
    yetenaweg.y_max,
    yetenaweg.confidence,
    yetenaweg.class_id,
    yetenaweg.class_name,
    yetenaweg.timestamp
from yetenaweg

union all

select
    lobelia4cosmetics.id,
    lobelia4cosmetics.image_id,
    lobelia4cosmetics.x_min,
    lobelia4cosmetics.y_min,
    lobelia4cosmetics.x_max,
    lobelia4cosmetics.y_max,
    lobelia4cosmetics.confidence,
    lobelia4cosmetics.class_id,
    lobelia4cosmetics.class_name,
    lobelia4cosmetics.timestamp
from lobelia4cosmetics