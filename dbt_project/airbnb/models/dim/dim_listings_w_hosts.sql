-- Business Requirements : 
-- For reporting and dashboards, we need a single listings table enriched with host attributes so analysts don’t have to join listings ↔ hosts every time.

-- The dashboard must slice listings by room_type, price, minimum_nights, and also show host name and superhost status.

-- We need an accurate ‘last updated’ timestamp for the combined record (listing or host changes).

-- Keep all listings even if host data is missing (so we don’t drop listings from KPIs).

WITH
l AS (
    SELECT
        *
    FROM
        {{ ref('dim_listings_cleansed') }}
),
h AS (
    SELECT * 
    FROM {{ ref('dim_hosts_cleansed') }}
)

SELECT 
    l.listing_id,
    l.listing_name,
    l.room_type,
    l.minimum_nights,
    l.price,
    l.host_id,
    h.host_name,
    h.is_superhost as host_is_superhost,
    l.created_at,
    GREATEST(l.updated_at, COALESCE(h.updated_at, l.updated_at)) AS updated_at
FROM l
LEFT JOIN h ON (h.host_id = l.host_id)