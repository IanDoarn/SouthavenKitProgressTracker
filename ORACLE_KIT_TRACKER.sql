WITH S0 AS (
    SELECT DISTINCT
      S.PRODUCT_ID,
      PC.COMPONENT_PRODUCT_ID,
      PC.QUANTITY
    FROM
      SMS_WRITE.STOCK S
      LEFT JOIN SMS_WRITE.PRODUCT_COMPONENT PC ON S.PRODUCT_ID = PC.PRODUCT_ID
    WHERE
      STOCK_TYPE IN (3, 4)
      AND INVENTORY_TYPE = 3
),
    S1 AS (
      SELECT
        P.ID                     AS KIT_PRODUCT_ID,
        P.PRODUCT_NUMBER         AS KIT_PRODUCT_NUMBER,
        P.EDI_NUMBER             AS KIT_EDI,
        P.DESCRIPTION            AS KIT_DESCRIPTION,
        P2.PRODUCT_NUMBER        AS COMPONENT_PRODUCT_NUMBER,
        P2.ID                    AS COMPONENT_PROD_ID,
        P2.EDI_NUMBER            AS COMPONENT_EDI_NUMBER,
        P2.DESCRIPTION           AS COMPONENT_DESCRIPTION,
        S0.QUANTITY              AS COMPONENT_QUANTITY_IN_KIT,
        COUNT(P2.PRODUCT_NUMBER) AS TOTAL_COMPONENT_QTY_IN_KIT
      FROM
        S0
        LEFT JOIN SMS_WRITE.PRODUCT P ON S0.PRODUCT_ID = P.ID
        LEFT JOIN SMS_WRITE.PRODUCT P2 ON S0.COMPONENT_PRODUCT_ID = P2.ID
      WHERE
        P.PRODUCT_NUMBER NOT LIKE 'ZPB%'
      GROUP BY
        P.ID, P.PRODUCT_NUMBER, P.EDI_NUMBER, P.DESCRIPTION,
        P2.ID, P2.EDI_NUMBER, P2.PRODUCT_NUMBER, P2.DESCRIPTION,
        S0.QUANTITY
  ),

    S5 AS (
      SELECT
        P2.PRODUCT_NUMBER                             AS KIT_PROD_NUMBER,
        PS.SERIAL_NUMBER                              AS SERIAL_NUMBER,
        B.ZONE || '-' || B.POSITION || '-' || B.SHELF AS KIT_BIN,
        P.EDI_NUMBER                                  AS COMPONENT_EDI,
        SUM(S.QUANTITY_AVAILABLE)                     AS QUANTITY_AVAILABLE,
        PL.LOT_NUMBER                                 AS COMPONENT_LOT_NUMBER,
        ZTAG.TAG_NUMBER                               AS COMPONENT_ZTAG
      FROM
        SMS_WRITE.STOCK S
        LEFT JOIN SMS_WRITE.PRODUCT P ON S.PRODUCT_ID = P.ID
        LEFT JOIN SMS_WRITE.STOCK S2 ON S.CONTAINER_ID = S2.ID AND S.CONTAINER_TYPE = 2
        LEFT JOIN SMS_WRITE.PRODUCT P2 ON S2.PRODUCT_ID = P2.ID
        LEFT JOIN SMS_WRITE.PRODUCT_SERIAL PS ON S2.SERIAL_ID = PS.ID
        LEFT JOIN SMS_WRITE.BIN B ON S2.CONTAINER_ID = B.ID AND S2.CONTAINER_TYPE = 1
        LEFT JOIN SMS_WRITE.PRODUCT_LOT PL ON S.LOT_ID = PL.ID
        LEFT JOIN SMS_WRITE.RFID_TAG ZTAG ON ZTAG.PRODUCT_LOT_ID = S.LOT_ID AND ZTAG.LAST_KNOWN_CONTAINER_TYPE = 2
      WHERE
        S.LOCATION_TYPE = 1
        AND S.LOCATION_ID = 370
        AND S.STOCK_TYPE = 2
        AND S.CONTAINER_TYPE = 2
        AND P2.PRODUCT_NUMBER IS NOT NULL
        AND ZTAG.LAST_KNOWN_CONTAINER_ID = S.CONTAINER_ID
      GROUP BY
        ZTAG.TAG_NUMBER,
        PL.LOT_NUMBER,
        P2.PRODUCT_NUMBER,
        PS.SERIAL_NUMBER,
        B.ZONE || '-' || B.POSITION || '-' || B.SHELF,
        P.EDI_NUMBER
  ),
    S7 AS (
      SELECT DISTINCT
        P.PRODUCT_NUMBER                              AS KIT_PRODUCT_NUMBER,
        PS.ID                                         AS SERIAL_ID,
        PS.SERIAL_NUMBER,
        S.HOLD_REASON,
        S.HOLD_NOTE,
        B.ZONE || '-' || B.POSITION || '-' || B.SHELF AS KIT_BIN,
        S.ID AS STOCK_ID,
        S.CONTAINER_ID AS BIN_ID
      FROM
        SMS_WRITE.STOCK S
        LEFT JOIN SMS_WRITE.PRODUCT P ON S.PRODUCT_ID = P.ID
        LEFT JOIN SMS_WRITE.PRODUCT_SERIAL PS ON PS.ID = S.SERIAL_ID
        LEFT JOIN SMS_WRITE.BIN B ON B.ID = S.CONTAINER_ID AND S.CONTAINER_TYPE = 1
      WHERE
        S.INVENTORY_TYPE = 3
        AND S.STOCK_TYPE IN (3, 4)
        AND S.LOCATION_TYPE = 1
        AND S.LOCATION_ID = 370
  ),
  S8 AS (
    SELECT
      P.PRODUCT_NUMBER,
      P.ID AS COMPONENT_ID,
      S.QUANTITY_AVAILABLE,
      B.ZONE || '-' || B.POSITION || '-' || B.SHELF AS COMPONENT_BIN
    FROM
      SMS_WRITE.STOCK S
      LEFT JOIN SMS_WRITE.PRODUCT P ON S.PRODUCT_ID = P.ID
      LEFT JOIN SMS_WRITE.BIN B ON S.CONTAINER_ID = B.ID AND S.CONTAINER_TYPE = 1
    WHERE
      S.LOCATION_ID = 370
      AND S.STOCK_TYPE IN (1, 2)
      AND S.QUANTITY_AVAILABLE > 0
      AND B.LOCATION_ID = 370
  ),
    S9 AS (
      SELECT
        S1.KIT_PRODUCT_NUMBER || ' (' || S7.SERIAL_NUMBER || ')' AS KIT_NAME,
        S1.KIT_PRODUCT_ID,
        S1.KIT_PRODUCT_NUMBER,
        S1.KIT_EDI,
        S1.KIT_DESCRIPTION,
        S7.SERIAL_NUMBER,
        S7.SERIAL_ID,
        S7.KIT_BIN,
        S7.STOCK_ID,
        S7.BIN_ID,
        S1.COMPONENT_PRODUCT_NUMBER,
        S1.COMPONENT_PROD_ID,
        S1.COMPONENT_EDI_NUMBER,
        S1.COMPONENT_DESCRIPTION,
        S1.COMPONENT_QUANTITY_IN_KIT,
        S1.TOTAL_COMPONENT_QTY_IN_KIT,
        S7.HOLD_REASON,
        S7.HOLD_NOTE
      FROM
        S1
        LEFT JOIN S7 ON S1.KIT_PRODUCT_NUMBER = S7.KIT_PRODUCT_NUMBER
  ),
    TRANSFERS AS (
      SELECT DISTINCT
        S9.KIT_PRODUCT_ID,
        S9.SERIAL_ID,
        COUNT(DISTINCT T.ID)
        OVER (
          PARTITION BY S9.SERIAL_ID ) AS NUMBER_OF_TRANSFERS_IN
      FROM
        SMS_WRITE.TRANSFER T
        LEFT JOIN S9 ON T.TO_KIT_PRODUCT_ID = S9.KIT_PRODUCT_ID AND T.TO_KIT_SERIAL_ID = S9.SERIAL_ID
      WHERE
        T.TRANSFER_TYPE IN (1, 13)
        AND T.LOCATION_ID = 370
        AND T.CREATED_DATE > CURRENT_DATE - 14
        AND T.STATUS IN (0, 1)
        AND S9.KIT_PRODUCT_ID IS NOT NULL
  ),
    WRAPUP AS (
      SELECT
        S9.KIT_NAME,
        S9.KIT_PRODUCT_ID                                                                       AS KIT_PRODUCT_ID,
        'Z-' || S9.KIT_PRODUCT_ID || '-' || S9.SERIAL_NUMBER                                    AS KIT_BARCODE,
        S9.KIT_PRODUCT_NUMBER                                                                   AS KIT_PRODUCT_NUMBER,
        S9.KIT_EDI                                                                              AS KIT_EDI_NUMBER,
        S9.KIT_DESCRIPTION                                                                      AS KIT_DESCRIPTION,
        S9.SERIAL_NUMBER                                                                        AS KIT_SERIAL_NUMBER,
        S9.SERIAL_ID                                                                            AS KIT_SERIAL_ID,
        S9.KIT_BIN,
        COALESCE(TRFS.NUMBER_OF_TRANSFERS_IN,
                 0)                                                                             AS NUMBER_OF_TRANSFERS_IN,
        S9.COMPONENT_PRODUCT_NUMBER                                                             AS COMPONENT_PRODUCT_NUMBER,
        S9.COMPONENT_PROD_ID                                                                    AS COMPONENT_PRODUCT_ID,
        COALESCE(S5.COMPONENT_LOT_NUMBER, NULL)                                                 AS COMPONENT_LOT_NUMBER,
        S5.COMPONENT_ZTAG                                                                       AS COMPONENT_ZTAG,
        S9.COMPONENT_DESCRIPTION                                                                AS COMPONENT_DESCRIPTION,
        COALESCE(S8.QUANTITY_AVAILABLE, 0)                                                      AS COMPONENT_QTY_ON_SHELF,
        COALESCE(S8.COMPONENT_BIN, NULL)                                                        AS COMPONENT_BIN,
        S9.COMPONENT_QUANTITY_IN_KIT                                                            AS COMPONENT_QUANTITY_IN_KIT_STD,
        COUNT(*)
        OVER (
          PARTITION BY S9.TOTAL_COMPONENT_QTY_IN_KIT, S9.SERIAL_NUMBER, S9.KIT_PRODUCT_NUMBER ) AS TOTAL_COMPONENTS_IN_KIT_STD,
        COALESCE(S5.QUANTITY_AVAILABLE, 0)                                                      AS QTY_IN_KIT,
        SUM(S9.COMPONENT_QUANTITY_IN_KIT - COALESCE(S5.QUANTITY_AVAILABLE, 0))
        OVER (
          PARTITION BY S9.KIT_PRODUCT_NUMBER, S9.SERIAL_NUMBER )                                AS PIECES_MISSING,
        S9.HOLD_REASON,
        S9.HOLD_NOTE,
        S9.STOCK_ID AS KIT_STOCK_ID,
        S9.BIN_ID AS KIT_CONTAINER_ID
      FROM
        S9
        LEFT JOIN S5 ON S9.KIT_PRODUCT_NUMBER = S5.KIT_PROD_NUMBER AND S9.SERIAL_NUMBER = S5.SERIAL_NUMBER AND
                        S9.COMPONENT_EDI_NUMBER = S5.COMPONENT_EDI
        LEFT JOIN TRANSFERS TRFS ON S9.SERIAL_ID = TRFS.SERIAL_ID
        LEFT JOIN S8 ON S8.COMPONENT_ID = S9.COMPONENT_PROD_ID
      WHERE
        S9.KIT_PRODUCT_NUMBER IN {}
        AND s9.SERIAL_NUMBER IN {}
      GROUP BY
        TRFS.NUMBER_OF_TRANSFERS_IN,
        S9.KIT_NAME,
        S9.HOLD_REASON,
        S9.HOLD_NOTE,
        S5.COMPONENT_ZTAG,
        S9.KIT_PRODUCT_ID,
        S9.SERIAL_NUMBER,
        S9.SERIAL_ID,
        S9.KIT_PRODUCT_NUMBER,
        S9.KIT_DESCRIPTION,
        S9.KIT_EDI,
        S9.KIT_BIN,
        S9.COMPONENT_EDI_NUMBER,
        S8.QUANTITY_AVAILABLE,
        S8.COMPONENT_BIN,
        S9.COMPONENT_QUANTITY_IN_KIT,
        S9.TOTAL_COMPONENT_QTY_IN_KIT,
        S9.COMPONENT_DESCRIPTION,
        S9.COMPONENT_PROD_ID,
        S9.COMPONENT_PRODUCT_NUMBER,
        S5.COMPONENT_LOT_NUMBER,
        S5.QUANTITY_AVAILABLE,
        S9.STOCK_ID,
        S9.BIN_ID
      ORDER BY
        S9.SERIAL_NUMBER,
        S9.COMPONENT_PRODUCT_NUMBER
  ),
  FINAL_TABLE AS (
      SELECT DISTINCT
        KIT_NAME,
        KIT_BARCODE,
        KIT_PRODUCT_ID,
        KIT_PRODUCT_NUMBER,
        KIT_DESCRIPTION,
        KIT_SERIAL_NUMBER,
        KIT_SERIAL_ID,
        KIT_BIN,
        KIT_STOCK_ID,
        KIT_CONTAINER_ID,
        COMPONENT_PRODUCT_ID,
        COMPONENT_PRODUCT_NUMBER,
        COMPONENT_DESCRIPTION,
        COMPONENT_LOT_NUMBER,
        LISTAGG(COMPONENT_ZTAG, ',')
        WITHIN GROUP (
          ORDER BY COMPONENT_ZTAG) ZTAGS,
        NUMBER_OF_TRANSFERS_IN,
        COMPONENT_QTY_ON_SHELF,
        COMPONENT_BIN,
        COMPONENT_QUANTITY_IN_KIT_STD,
        TOTAL_COMPONENTS_IN_KIT_STD,
        TOTAL_COMPONENTS_IN_KIT_STD - PIECES_MISSING AS TOTAL_PIECES_IN_KIT,
        QTY_IN_KIT AS COMPONENT_QTY_IN_KIT,
        PIECES_MISSING,
        CASE
        WHEN PIECES_MISSING > 0 AND PIECES_MISSING < TOTAL_COMPONENTS_IN_KIT_STD AND KIT_BIN != 'New Kit Assembly-0-0'
             AND
             NUMBER_OF_TRANSFERS_IN = 0
          THEN 'INVALID'
        WHEN PIECES_MISSING = 0 AND KIT_BIN != 'New Kit Assembly-0-0'
          THEN 'VALID'
        WHEN PIECES_MISSING = TOTAL_COMPONENTS_IN_KIT_STD AND KIT_BIN != 'New Kit Assembly-0-0'
          THEN 'EMPTY'
        WHEN PIECES_MISSING = TOTAL_COMPONENTS_IN_KIT_STD AND KIT_BIN = 'New Kit Assembly-0-0' AND
             NUMBER_OF_TRANSFERS_IN > 0
          THEN 'NEW_KIT_BUILD'
        WHEN PIECES_MISSING > 0 AND NUMBER_OF_TRANSFERS_IN > 0 AND KIT_BIN != 'New Kit Assembly-0-0'
          THEN 'REPLENISHMENT'
        WHEN PIECES_MISSING = TOTAL_COMPONENTS_IN_KIT_STD AND KIT_BIN = 'New Kit Assembly-0-0' AND
             NUMBER_OF_TRANSFERS_IN = 0
          THEN 'AWAITING_STOCK'
        WHEN PIECES_MISSING > 0 AND PIECES_MISSING < TOTAL_COMPONENTS_IN_KIT_STD AND KIT_BIN = 'New Kit Assembly-0-0'
             AND
             NUMBER_OF_TRANSFERS_IN = 0
          THEN 'AWAITING_PUTAWAY'
        WHEN PIECES_MISSING = 0 AND KIT_BIN = 'New Kit Assembly-0-0' AND
             NUMBER_OF_TRANSFERS_IN = 0
          THEN 'AWAITING_PUTAWAY'
        ELSE 'MANUEL_INVESTIGATION_REQUIRED'
        END AS                     KIT_HEALTH,
        HOLD_NOTE,
        HOLD_REASON,
        CASE
        WHEN HOLD_REASON = 1
          THEN 'CORPORATE_HOLD'
        WHEN HOLD_REASON = 2
          THEN 'AWAITING_QC_CHECK'
        WHEN HOLD_REASON = 3
          THEN 'INVENTORY_STAGING'
        WHEN HOLD_REASON = 4
          THEN 'MISSING_ITEMS'
        WHEN HOLD_REASON = 5
          THEN 'PICK_SHORTAGE'
        WHEN HOLD_REASON = 6
          THEN 'CYCLE_COUNT_IN_PROGRESS'
        WHEN HOLD_REASON = 7
          THEN 'NOT_FOUND_DURING_CYCLE_COUNT'
        END AS                     HOLD_REASON_DESCRIPTION
      FROM
        WRAPUP
      GROUP BY
        KIT_NAME,
        KIT_CONTAINER_ID,
        KIT_STOCK_ID,
        HOLD_NOTE,
        HOLD_REASON,
        KIT_PRODUCT_ID,
        KIT_BARCODE,
        KIT_PRODUCT_NUMBER,
        KIT_DESCRIPTION,
        KIT_SERIAL_NUMBER,
        KIT_SERIAL_ID,
        KIT_BIN,
        COMPONENT_BIN,
        NUMBER_OF_TRANSFERS_IN,
        COMPONENT_QTY_ON_SHELF,
        COMPONENT_PRODUCT_ID,
        COMPONENT_PRODUCT_NUMBER,
        COMPONENT_DESCRIPTION,
        COMPONENT_LOT_NUMBER,
        COMPONENT_QUANTITY_IN_KIT_STD,
        TOTAL_COMPONENTS_IN_KIT_STD,
        QTY_IN_KIT,
        PIECES_MISSING
      ORDER BY
        KIT_PRODUCT_NUMBER
  ),
ORGANIZE AS (
      SELECT DISTINCT
        KIT_BARCODE,
        KIT_PRODUCT_NUMBER,
        KIT_SERIAL_NUMBER,
        CASE
          WHEN KIT_BIN = '--'
            THEN 'OTHER_BIN'
          ELSE KIT_BIN
        END AS KIT_BIN,
        KIT_HEALTH,
        CASE
          WHEN COMPONENT_QTY_ON_SHELF > 0 AND PIECES_MISSING - COMPONENT_QTY_ON_SHELF = 0 AND KIT_HEALTH NOT IN ('BUILDING', 'VALID')
            THEN 'STOCK_AVAILABLE'
          WHEN KIT_HEALTH = 'NEW_KIT_BUILD'
            THEN 'COMPLETE_NEW_KIT_BUILD'
          WHEN KIT_HEALTH = 'REPLENISHMENT'
            THEN 'COMPLETE_REPLENISHMENTS'
          WHEN COMPONENT_QTY_ON_SHELF > 0 AND PIECES_MISSING - COMPONENT_QTY_ON_SHELF != 0 AND KIT_HEALTH = 'INVALID'
            THEN 'NO_STOCK_AVAILABLE'
          WHEN SUM(COMPONENT_QTY_ON_SHELF) = 0 AND KIT_HEALTH != 'VALID'
            THEN 'NO_STOCK_AVAILABLE'
          WHEN KIT_HEALTH = 'VALID'
            THEN 'NO_ACTION_REQUIRED'
          WHEN KIT_HEALTH = 'AWAITING_STOCK' AND PIECES_MISSING - COMPONENT_QTY_ON_SHELF != 0 AND KIT_HEALTH != 'VALID'
            THEN 'NO_STOCK_AVAILABLE'
          WHEN KIT_HEALTH = 'AWAITING_PUTAWAY'
            THEN 'COMPLETE_PUTAWAY'
          WHEN HOLD_REASON > 0
            THEN HOLD_REASON_DESCRIPTION
          ELSE KIT_HEALTH
        END AS POTENTIAL_ACTION_ON_KIT,
        KIT_CONTAINER_ID,
        KIT_SERIAL_ID,
        KIT_PRODUCT_ID,
        KIT_STOCK_ID
      FROM
        FINAL_TABLE
      GROUP BY
        KIT_HEALTH, HOLD_REASON, HOLD_REASON_DESCRIPTION,
        KIT_BARCODE, KIT_PRODUCT_NUMBER, KIT_SERIAL_NUMBER,
        TOTAL_PIECES_IN_KIT, COMPONENT_QUANTITY_IN_KIT_STD, COMPONENT_QTY_ON_SHELF,
        PIECES_MISSING, TOTAL_COMPONENTS_IN_KIT_STD, KIT_BIN,
        NUMBER_OF_TRANSFERS_IN, COMPONENT_QTY_IN_KIT, KIT_STOCK_ID,
        KIT_CONTAINER_ID, KIT_SERIAL_ID, KIT_PRODUCT_ID
      ORDER BY
        KIT_PRODUCT_NUMBER,
        KIT_SERIAL_NUMBER
  )
SELECT DISTINCT *
FROM ORGANIZE
WHERE KIT_SERIAL_NUMBER IS NOT NULL