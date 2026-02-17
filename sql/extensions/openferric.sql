CREATE SCHEMA IF NOT EXISTS openferric;

CREATE OR REPLACE FUNCTION openferric.european_call(
    spot float8,
    strike float8,
    expiry float8,
    vol float8,
    rate float8
) RETURNS float8
AS $$
BEGIN
    RETURN bs_price(spot, strike, expiry, vol, rate, 'call');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION openferric.european_put(
    spot float8,
    strike float8,
    expiry float8,
    vol float8,
    rate float8
) RETURNS float8
AS $$
BEGIN
    RETURN bs_price(spot, strike, expiry, vol, rate, 'put');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION openferric.european_greeks(
    spot float8,
    strike float8,
    expiry float8,
    vol float8,
    rate float8,
    option_type text
) RETURNS jsonb
AS $$
BEGIN
    RETURN bs_greeks(spot, strike, expiry, vol, rate, option_type);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION openferric.american_call(
    spot float8,
    strike float8,
    expiry float8,
    vol float8,
    rate float8,
    steps int DEFAULT 500
) RETURNS float8
AS $$
BEGIN
    RETURN american_price(spot, strike, expiry, vol, rate, 'call', steps);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION openferric.american_put(
    spot float8,
    strike float8,
    expiry float8,
    vol float8,
    rate float8,
    steps int DEFAULT 500
) RETURNS float8
AS $$
BEGIN
    RETURN american_price(spot, strike, expiry, vol, rate, 'put', steps);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION openferric.barrier(
    spot float8,
    strike float8,
    expiry float8,
    vol float8,
    rate float8,
    barrier float8,
    option_type text,
    barrier_type text,
    barrier_dir text
) RETURNS float8
AS $$
BEGIN
    RETURN barrier_price(
        spot,
        strike,
        expiry,
        vol,
        rate,
        barrier,
        option_type,
        barrier_type,
        barrier_dir
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION openferric.heston(
    spot float8,
    strike float8,
    expiry float8,
    rate float8,
    v0 float8,
    kappa float8,
    theta float8,
    sigma_v float8,
    rho float8,
    option_type text
) RETURNS float8
AS $$
BEGIN
    RETURN heston_price(
        spot,
        strike,
        expiry,
        rate,
        v0,
        kappa,
        theta,
        sigma_v,
        rho,
        option_type
    );
END;
$$ LANGUAGE plpgsql;
