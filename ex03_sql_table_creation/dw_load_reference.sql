-- dim_payment_type
INSERT INTO dim_payment_type VALUES
(0, 'Flex Fare'),
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip')
ON CONFLICT DO NOTHING;

-- dim_ratecode
INSERT INTO dim_ratecode VALUES
(1, 'Standard rate'),
(2, 'JFK'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride'),
(99, 'Unknown')
ON CONFLICT DO NOTHING;

-- dim_vendor
INSERT INTO dim_vendor VALUES
(1, 'Creative Mobile Technologies'),
(2, 'Curb Mobility'),
(6, 'Myle Technologies'),
(7, 'Helix')
ON CONFLICT DO NOTHING;
