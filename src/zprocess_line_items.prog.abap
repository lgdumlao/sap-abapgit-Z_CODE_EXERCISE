*&---------------------------------------------------------------------*
*& Report ZPROCESS_LINE_ITEMS
*&---------------------------------------------------------------------*
*& Reads a block of line items from staging table ZLINE_STAGE
*& identified by p_memid (run_id), inserts them into ZLINE_ITEMS,
*& skipping any line_id that already exists. Cleans up staging rows.
*&---------------------------------------------------------------------*
REPORT zprocess_line_items.

*----------------------------------------------------------------------*
* PARAMETERS  (populated programmatically by scheduler via SUBMIT WITH)
*----------------------------------------------------------------------*


PARAMETERS: p_memid TYPE c LENGTH 30 LOWER CASE.

*----------------------------------------------------------------------*
* CLASS: ZCL_LINE_ITEM_PROCESSOR
*----------------------------------------------------------------------*
CLASS zcl_line_item_processor DEFINITION FINAL.

  PUBLIC SECTION.
    METHODS:
      constructor
        IMPORTING iv_run_id TYPE c,
      process
        RETURNING VALUE(rv_success) TYPE abap_bool.

  PRIVATE SECTION.
    TYPES:
      BEGIN OF ty_stage_item,
        line_id   TYPE zline_items-line_id,
        line_text TYPE zline_items-line_text,
      END OF ty_stage_item,
      tt_stage_items TYPE STANDARD TABLE OF ty_stage_item WITH DEFAULT KEY.

    DATA: mv_run_id TYPE c LENGTH 30,
          mt_items  TYPE tt_stage_items.

    METHODS:
      load_items_from_stage
        RETURNING VALUE(rv_success) TYPE abap_bool,
      insert_items,
      cleanup_stage.

ENDCLASS.

CLASS zcl_line_item_processor IMPLEMENTATION.

  METHOD constructor.
    mv_run_id = iv_run_id.
  ENDMETHOD.

  METHOD process.
    rv_success = abap_false.

    WRITE: / '--- ZPROCESS_LINE_ITEMS ---'.
    WRITE: / 'Run ID (memory ID):', mv_run_id.

    IF load_items_from_stage( ) = abap_false.
      RETURN.
    ENDIF.

    insert_items( ).
    cleanup_stage( ).

    rv_success = abap_true.
  ENDMETHOD.

  METHOD load_items_from_stage.
    rv_success = abap_false.

    IF mv_run_id IS INITIAL.
      WRITE: / 'ERROR: Run ID is empty. Cannot load from staging table.'.
      RETURN.
    ENDIF.

    " Read the block assigned to this job from the staging table
    SELECT line_id, line_text
      FROM zline_stage
      WHERE run_id = @mv_run_id
      INTO TABLE @mt_items.

    IF sy-subrc <> 0 OR mt_items IS INITIAL.
      WRITE: / 'WARNING: No items found in staging table for run_id:', mv_run_id.
      RETURN.
    ENDIF.

    WRITE: / 'INFO: Loaded', lines( mt_items ), 'item(s) from staging table.'.
    rv_success = abap_true.
  ENDMETHOD.

  METHOD insert_items.
    DATA: ls_zline    TYPE zline_items,
          lt_insert   TYPE STANDARD TABLE OF zline_items WITH DEFAULT KEY,
          lt_existing TYPE STANDARD TABLE OF zline_items WITH DEFAULT KEY,
          lv_skipped  TYPE i VALUE 0,
          lv_inserted TYPE i VALUE 0.

    " Read all existing line_ids in one shot (FOR ALL ENTRIES)
    SELECT line_id
      FROM zline_items
      FOR ALL ENTRIES IN @mt_items
      WHERE line_id = @mt_items-line_id
      INTO TABLE @lt_existing.

    " Build insert table, skipping already existing line_ids
    LOOP AT mt_items INTO DATA(ls_item).
      READ TABLE lt_existing WITH KEY line_id = ls_item-line_id
           TRANSPORTING NO FIELDS.
      IF sy-subrc = 0.
        lv_skipped = lv_skipped + 1.
        WRITE: / 'SKIP: line_id', ls_item-line_id, 'already exists.'.
        CONTINUE.
      ENDIF.

      CLEAR ls_zline.
      ls_zline-line_id   = ls_item-line_id.
      ls_zline-line_text = ls_item-line_text.
      GET TIME STAMP FIELD ls_zline-changed_at.
      ls_zline-changed_by = sy-uname.
      APPEND ls_zline TO lt_insert.
    ENDLOOP.

    " Bulk insert
    IF lt_insert IS NOT INITIAL.
      INSERT zline_items FROM TABLE lt_insert.
      IF sy-subrc = 0.
        lv_inserted = lines( lt_insert ).
      ELSE.
        WRITE: / 'ERROR: INSERT into ZLINE_ITEMS failed. SY-SUBRC =', sy-subrc.
      ENDIF.
    ENDIF.

    WRITE: / 'SUMMARY: Inserted =', lv_inserted, '| Skipped =', lv_skipped.
  ENDMETHOD.

  METHOD cleanup_stage.
    " Remove processed staging rows to keep the table clean
    DELETE FROM zline_stage WHERE run_id = @mv_run_id.

    IF sy-subrc = 0.
      WRITE: / 'INFO: Staging rows cleaned up for run_id:', mv_run_id.
    ELSE.
      WRITE: / 'WARNING: Staging cleanup returned SY-SUBRC =', sy-subrc,
               ' for run_id:', mv_run_id.
    ENDIF.
  ENDMETHOD.

ENDCLASS.

*----------------------------------------------------------------------*
* MAIN PROCESSING
*----------------------------------------------------------------------*
START-OF-SELECTION.

  IF p_memid IS INITIAL.
    WRITE: / 'ERROR: No run ID (p_memid) provided.',
           / 'This program must be called by ZPROCESS_LINE_ITEMS_SCHEDULER.'.
    RETURN.
  ENDIF.

*    BREAK CM7984.

  DATA(lo_processor) = NEW zcl_line_item_processor( p_memid ).
  lo_processor->process( ).
