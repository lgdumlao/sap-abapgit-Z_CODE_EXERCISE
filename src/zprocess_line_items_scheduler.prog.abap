*&---------------------------------------------------------------------*
*& Report ZPROCESS_LINE_ITEMS_SCHEDULER
*&---------------------------------------------------------------------*
*& Uploads a CSV/TXT file, splits lines into blocks, and schedules
*& background jobs for ZPROCESS_LINE_ITEMS per block.
*& Displays scheduling results in ALV.
*&---------------------------------------------------------------------*
REPORT zprocess_line_items_scheduler.

*----------------------------------------------------------------------*
* TYPE DEFINITIONS
*----------------------------------------------------------------------*
TYPES:
  BEGIN OF ty_file_line,
    line_id   TYPE zline_items-line_id,
    line_text TYPE zline_items-line_text,
  END OF ty_file_line,
  tt_file_lines TYPE STANDARD TABLE OF ty_file_line WITH DEFAULT KEY,

  BEGIN OF ty_alv_result,
    icon       TYPE c LENGTH 4,
    job_name   TYPE btcjob,
    sched_date TYPE d,
    sched_time TYPE t,
    block_from TYPE i,
    block_to   TYPE i,
    message    TYPE string,
  END OF ty_alv_result,
  tt_alv_results TYPE STANDARD TABLE OF ty_alv_result WITH DEFAULT KEY.

*----------------------------------------------------------------------*
* SELECTION SCREEN
*----------------------------------------------------------------------*
SELECTION-SCREEN BEGIN OF BLOCK blk_file WITH FRAME TITLE TEXT-t01.
  PARAMETERS: p_file TYPE localfile DEFAULT 'C:\Users\dumlao.ld\Downloads\code\excel_file.txt' OBLIGATORY.
SELECTION-SCREEN END OF BLOCK blk_file.

SELECTION-SCREEN BEGIN OF BLOCK blk_proc WITH FRAME TITLE TEXT-t02.
  PARAMETERS: p_blksz TYPE i DEFAULT 5 OBLIGATORY.
SELECTION-SCREEN END OF BLOCK blk_proc.

SELECTION-SCREEN BEGIN OF BLOCK blk_sched WITH FRAME TITLE TEXT-t03.
  PARAMETERS:
    p_immed  RADIOBUTTON GROUP rb1 DEFAULT 'X' USER-COMMAND ucom,
    p_sched  RADIOBUTTON GROUP rb1.
  PARAMETERS:
    p_date   LIKE sy-datum,
    p_time   LIKE sy-uzeit.
SELECTION-SCREEN END OF BLOCK blk_sched.

*----------------------------------------------------------------------*
* SELECTION SCREEN EVENTS
*----------------------------------------------------------------------*
AT SELECTION-SCREEN OUTPUT.
  LOOP AT SCREEN.
    IF screen-name = 'P_DATE' OR screen-name = 'P_TIME'.
      IF p_sched = 'X'.
        screen-active = 1.
        screen-input  = 1.
      ELSE.
        screen-active = 0.
        screen-input  = 0.
      ENDIF.
      MODIFY SCREEN.
    ENDIF.
  ENDLOOP.

AT SELECTION-SCREEN.
*  IF p_sched = 'X'.
*    IF p_date IS INITIAL.
*      MESSAGE 'Please enter a schedule date.' TYPE 'E'.
*    ENDIF.
*    IF p_time IS INITIAL.
*      MESSAGE 'Please enter a schedule time.' TYPE 'E'.
*    ENDIF.
*    IF p_date < sy-datum.
*      MESSAGE 'Schedule date cannot be in the past.' TYPE 'E'.
*    ENDIF.
*  ENDIF.
*  IF p_blksz <= 0.
*    MESSAGE 'Block size must be greater than 0.' TYPE 'E'.
*  ENDIF.

*----------------------------------------------------------------------*
* CLASS: ZCL_BTC_CHECK
*----------------------------------------------------------------------*
*CLASS zcl_btc_check DEFINITION FINAL.
*  PUBLIC SECTION.
*    CLASS-METHODS:
*      get_free_processes
*        RETURNING VALUE(rv_free) TYPE i,
*      wait_for_free_processes
*        IMPORTING iv_min_free TYPE i DEFAULT 2
*                  iv_max_wait TYPE i DEFAULT 300.
*ENDCLASS.
*
*CLASS zcl_btc_check IMPLEMENTATION.
*
*  METHOD get_free_processes.
*    DATA: lv_total  TYPE i VALUE 0,
*          lv_active TYPE i VALUE 0,
*          lt_wpinfo TYPE STANDARD TABLE OF wpinfo WITH DEFAULT KEY,
*          ls_wpinfo TYPE wpinfo.
*
*    CALL FUNCTION 'TH_GET_WPINFO'
*      TABLES
*        wpinfo = lt_wpinfo
*      EXCEPTIONS
*        OTHERS = 1.
*
*    IF sy-subrc = 0.
*      LOOP AT lt_wpinfo INTO ls_wpinfo WHERE wp_typ = 'BTC'.
*        lv_total = lv_total + 1.
*      ENDLOOP.
*    ENDIF.
*
*    IF lv_total = 0.
*      lv_total = 3.  " conservative fallback
*    ENDIF.
*
*    SELECT COUNT(*) FROM tbtco
*      INTO @lv_active
*      WHERE status = 'R'.
*
*    rv_free = lv_total - lv_active.
*    IF rv_free < 0. rv_free = 0. ENDIF.
*  ENDMETHOD.
*
*  METHOD wait_for_free_processes.
*    DATA: lv_waited   TYPE i VALUE 0,
*          lv_free     TYPE i,
*          lv_interval TYPE i VALUE 10.
*
*    DO.
*      lv_free = get_free_processes( ).
*      IF lv_free >= iv_min_free.
*        RETURN.
*      ENDIF.
*      IF lv_waited >= iv_max_wait.
*        WRITE: / 'WARNING: Max wait time exceeded. Proceeding anyway.'.
*        RETURN.
*      ENDIF.
*      WRITE: / 'Waiting for free BTC processes... Free:', lv_free,
*               '| Waited:', lv_waited, 's'.
*      WAIT UP TO lv_interval SECONDS.
*      lv_waited = lv_waited + lv_interval.
*    ENDDO.
*  ENDMETHOD.
*
*ENDCLASS.

*----------------------------------------------------------------------*
* CLASS: ZCL_FILE_READER
*----------------------------------------------------------------------*
CLASS zcl_file_reader DEFINITION FINAL.
  PUBLIC SECTION.
    METHODS:
      read_file
        IMPORTING iv_filepath  TYPE localfile
        EXPORTING et_lines     TYPE tt_file_lines
        RETURNING VALUE(rv_ok) TYPE abap_bool.
ENDCLASS.

CLASS zcl_file_reader IMPLEMENTATION.
  METHOD read_file.
    DATA: lt_raw      TYPE TABLE OF string,
          lv_line     TYPE string,
          ls_item     TYPE ty_file_line,
          lv_parts    TYPE TABLE OF string,
          lv_filename TYPE string.

    rv_ok = abap_false.
    CLEAR et_lines.

    lv_filename = iv_filepath.

    cl_gui_frontend_services=>gui_upload(
      EXPORTING
        filename                = lv_filename
        filetype                = 'ASC'
      CHANGING
        data_tab                = lt_raw
      EXCEPTIONS
        file_open_error         = 1
        file_read_error         = 2
        no_batch                = 3
        gui_refuse_filetransfer = 4
        invalid_type            = 5
        OTHERS                  = 6
    ).

    IF sy-subrc <> 0.
      MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno
              WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
      RETURN.
    ENDIF.

    DATA(lv_first) = abap_true.

    LOOP AT lt_raw INTO lv_line.
      IF lv_first = abap_true.
        lv_first = abap_false.
        CONTINUE.
      ENDIF.

      CONDENSE lv_line.
      IF lv_line IS INITIAL. CONTINUE. ENDIF.

      CLEAR lv_parts.
      SPLIT lv_line AT ',' INTO TABLE lv_parts.

      IF lines( lv_parts ) < 2. CONTINUE. ENDIF.

      CLEAR ls_item.
      ls_item-line_id   = lv_parts[ 1 ].
      ls_item-line_text = lv_parts[ 2 ].
      CONDENSE ls_item-line_id.
      CONDENSE ls_item-line_text.
      APPEND ls_item TO et_lines.
    ENDLOOP.

    IF et_lines IS NOT INITIAL.
      rv_ok = abap_true.
    ENDIF.
  ENDMETHOD.
ENDCLASS.

*----------------------------------------------------------------------*
* CLASS: ZCL_JOB_SCHEDULER
*----------------------------------------------------------------------*
CLASS zcl_job_scheduler DEFINITION FINAL.
  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_block_info,
        block_no TYPE i,
        from_idx TYPE i,
        to_idx   TYPE i,
        items    TYPE tt_file_lines,
      END OF ty_block_info,
      tt_blocks TYPE STANDARD TABLE OF ty_block_info WITH DEFAULT KEY.

    METHODS:
      constructor
        IMPORTING
          iv_block_size  TYPE i
          iv_sched_immed TYPE abap_bool
          iv_sched_date  TYPE d
          iv_sched_time  TYPE t,
      build_blocks
        IMPORTING it_lines         TYPE tt_file_lines
        RETURNING VALUE(rt_blocks) TYPE tt_blocks,
      schedule_all
        IMPORTING it_blocks  TYPE tt_blocks
        EXPORTING et_results TYPE tt_alv_results.

  PRIVATE SECTION.
    DATA: mv_block_size  TYPE i,
          mv_sched_immed TYPE abap_bool,
          mv_sched_date  TYPE d,
          mv_sched_time  TYPE t,
          mv_run_date    TYPE d,
          mv_run_time    TYPE t.

    METHODS:
      schedule_single_block
        IMPORTING is_block  TYPE ty_block_info
        EXPORTING es_result TYPE ty_alv_result,
      build_job_name
        IMPORTING iv_block_no    TYPE i
        RETURNING VALUE(rv_name) TYPE btcjob,
      write_block_to_stage
        IMPORTING is_block     TYPE ty_block_info
                  iv_run_id    TYPE c
        RETURNING VALUE(rv_ok) TYPE abap_bool.
ENDCLASS.

CLASS zcl_job_scheduler IMPLEMENTATION.

  METHOD constructor.
    mv_block_size  = iv_block_size.
    mv_sched_immed = iv_sched_immed.
    mv_sched_date  = iv_sched_date.
    mv_sched_time  = iv_sched_time.

    IF mv_sched_immed = abap_true.
      mv_run_date = sy-datum.
      mv_run_time = sy-uzeit.
    ELSE.
      mv_run_date = iv_sched_date.
      mv_run_time = iv_sched_time.
    ENDIF.
  ENDMETHOD.

  METHOD build_blocks.
    DATA: ls_block TYPE ty_block_info,
          lv_total TYPE i,
          lv_idx   TYPE i VALUE 1,
          lv_blk   TYPE i VALUE 1.

    lv_total = lines( it_lines ).

    WHILE lv_idx <= lv_total.
      CLEAR ls_block.
      ls_block-block_no = lv_blk.
      ls_block-from_idx = lv_idx.
      ls_block-to_idx   = nmin( val1 = ( lv_idx + mv_block_size - 1 )
                                val2 = lv_total ).

      LOOP AT it_lines INTO DATA(ls_line)
           FROM ls_block-from_idx TO ls_block-to_idx.
        APPEND ls_line TO ls_block-items.
      ENDLOOP.

      APPEND ls_block TO rt_blocks.
      lv_idx = ls_block-to_idx + 1.
      lv_blk = lv_blk + 1.
    ENDWHILE.
  ENDMETHOD.

  METHOD schedule_all.
*   CONSTANTS: lc_min_free_btc TYPE i VALUE 2.  " removed - BTC check disabled

    LOOP AT it_blocks INTO DATA(ls_block).
*      zcl_btc_check=>wait_for_free_processes(
*        iv_min_free = lc_min_free_btc
*        iv_max_wait = 120
*      ).

      DATA(ls_result) = VALUE ty_alv_result( ).
      schedule_single_block(
        EXPORTING is_block  = ls_block
        IMPORTING es_result = ls_result
      ).
      APPEND ls_result TO et_results.
    ENDLOOP.
  ENDMETHOD.

  METHOD write_block_to_stage.
    DATA: lt_stage TYPE STANDARD TABLE OF zline_stage WITH DEFAULT KEY,
          ls_stage TYPE zline_stage.

    rv_ok = abap_false.

    LOOP AT is_block-items INTO DATA(ls_item).
      CLEAR ls_stage.
      ls_stage-run_id    = iv_run_id.
      ls_stage-line_id   = ls_item-line_id.
      ls_stage-line_text = ls_item-line_text.
      APPEND ls_stage TO lt_stage.
    ENDLOOP.

    IF lt_stage IS INITIAL.
      RETURN.
    ENDIF.

    INSERT zline_stage FROM TABLE lt_stage.

    IF sy-subrc = 0.
      rv_ok = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD schedule_single_block.
    DATA: lv_job_name  TYPE btcjob,
          lv_job_count TYPE btcjobcnt,
          lv_run_id    TYPE c LENGTH 30.

    lv_job_name = build_job_name( is_block-block_no ).
    lv_run_id   = lv_job_name.

    es_result-job_name   = lv_job_name.
    es_result-sched_date = mv_run_date.
    es_result-sched_time = mv_run_time.
    es_result-block_from = is_block-from_idx.
    es_result-block_to   = is_block-to_idx.

    DATA(lv_stage_ok) = write_block_to_stage(
                          is_block  = is_block
                          iv_run_id = lv_run_id ).

    IF lv_stage_ok = abap_false.
      es_result-icon    = '@0A@'.
      es_result-message = |ERROR: Failed to write block to staging table for run_id={ lv_run_id }|.
      RETURN.
    ENDIF.

    CALL FUNCTION 'JOB_OPEN'
      EXPORTING
        jobname          = lv_job_name
      IMPORTING
        jobcount         = lv_job_count
      EXCEPTIONS
        cant_create_job  = 1
        invalid_job_data = 2
        jobname_missing  = 3
        OTHERS           = 4.

    IF sy-subrc <> 0.
      es_result-icon    = '@0A@'.
      es_result-message = |ERROR: JOB_OPEN failed. SUBRC={ sy-subrc }|.
      DELETE FROM zline_stage WHERE run_id = @lv_run_id.
      RETURN.
    ENDIF.

    SUBMIT zprocess_line_items
      WITH p_memid = lv_run_id
      VIA JOB lv_job_name NUMBER lv_job_count
      AND RETURN.

    IF sy-subrc <> 0.
      es_result-icon    = '@0A@'.
      es_result-message = |ERROR: SUBMIT failed. SUBRC={ sy-subrc }|.
      CALL FUNCTION 'JOB_CLOSE'
        EXPORTING
          jobname    = lv_job_name
          jobcount   = lv_job_count
          strtimmed  = ' '
          sdlstrtdt  = '00000000'
          sdlstrttm  = '000000'
        EXCEPTIONS
          OTHERS     = 1.
      DELETE FROM zline_stage WHERE run_id = @lv_run_id.
      RETURN.
    ENDIF.

    IF mv_sched_immed = abap_true.
      CALL FUNCTION 'JOB_CLOSE'
        EXPORTING
          jobname              = lv_job_name
          jobcount             = lv_job_count
          strtimmed            = 'X'
        EXCEPTIONS
          cant_start_immediate = 1
          invalid_startdate    = 2
          jobname_missing      = 3
          job_close_failed     = 4
          job_nosteps          = 5
          job_notex            = 6
          lock_failed          = 7
          OTHERS               = 8.
    ELSE.
      CALL FUNCTION 'JOB_CLOSE'
        EXPORTING
          jobname              = lv_job_name
          jobcount             = lv_job_count
          strtimmed            = ' '
          sdlstrtdt            = mv_sched_date
          sdlstrttm            = mv_run_time
        EXCEPTIONS
          cant_start_immediate = 1
          invalid_startdate    = 2
          jobname_missing      = 3
          job_close_failed     = 4
          job_nosteps          = 5
          job_notex            = 6
          lock_failed          = 7
          OTHERS               = 8.
    ENDIF.

    IF sy-subrc <> 0.
      es_result-icon    = '@0A@'.
      es_result-message = |ERROR: JOB_CLOSE failed. SUBRC={ sy-subrc }|.
      DELETE FROM zline_stage WHERE run_id = @lv_run_id.
    ELSE.
      es_result-icon    = '@09@'.
      es_result-message = 'Successfully scheduled'.
    ENDIF.
  ENDMETHOD.

  METHOD build_job_name.
    DATA: lv_date TYPE c LENGTH 8,
          lv_time TYPE c LENGTH 6,
          lv_blk  TYPE c LENGTH 4.

    lv_date = mv_run_date.
    lv_time = mv_run_time.
    lv_blk  = |{ iv_block_no WIDTH = 4 ALIGN = RIGHT PAD = '0' }|.

    rv_name = |ZZBG_{ lv_date }_{ lv_time }_{ lv_blk }|.
  ENDMETHOD.

ENDCLASS.

*----------------------------------------------------------------------*
* CLASS: ZCL_ALV_DISPLAY
*----------------------------------------------------------------------*
CLASS zcl_alv_display DEFINITION FINAL.
  PUBLIC SECTION.
    CLASS-METHODS:
      display
        IMPORTING it_results TYPE tt_alv_results.
ENDCLASS.

CLASS zcl_alv_display IMPLEMENTATION.
  METHOD display.
    DATA: lo_alv     TYPE REF TO cl_salv_table,
          lo_cols    TYPE REF TO cl_salv_columns_table,
          lo_col     TYPE REF TO cl_salv_column_table,
          lo_funcs   TYPE REF TO cl_salv_functions_list,
          lo_layout  TYPE REF TO cl_salv_layout,
          ls_key     TYPE salv_s_layout_key,
          lt_results TYPE tt_alv_results.

    lt_results = it_results.

    TRY.
        cl_salv_table=>factory(
          IMPORTING
            r_salv_table = lo_alv
          CHANGING
            t_table      = lt_results
        ).

        lo_funcs = lo_alv->get_functions( ).
        lo_funcs->set_all( abap_true ).

        lo_layout = lo_alv->get_layout( ).
        ls_key-report = sy-repid.
        lo_layout->set_key( ls_key ).
        lo_layout->set_save_restriction( if_salv_c_layout=>restrict_none ).

        lo_cols = lo_alv->get_columns( ).
        lo_cols->set_optimize( abap_true ).

        lo_col ?= lo_cols->get_column( 'ICON' ).
        lo_col->set_long_text( 'Status' ).
        lo_col->set_medium_text( 'Status' ).
        lo_col->set_short_text( 'St.' ).
        lo_col->set_icon( if_salv_c_bool_sap=>true ).

        lo_col ?= lo_cols->get_column( 'JOB_NAME' ).
        lo_col->set_long_text( 'Job Name' ).
        lo_col->set_medium_text( 'Job Name' ).
        lo_col->set_short_text( 'Job Name' ).

        lo_col ?= lo_cols->get_column( 'SCHED_DATE' ).
        lo_col->set_long_text( 'Scheduled Date' ).
        lo_col->set_medium_text( 'Sched. Date' ).
        lo_col->set_short_text( 'Date' ).

        lo_col ?= lo_cols->get_column( 'SCHED_TIME' ).
        lo_col->set_long_text( 'Scheduled Time' ).
        lo_col->set_medium_text( 'Sched. Time' ).
        lo_col->set_short_text( 'Time' ).

        lo_col ?= lo_cols->get_column( 'BLOCK_FROM' ).
        lo_col->set_long_text( 'Block Start Index' ).
        lo_col->set_medium_text( 'Block From' ).
        lo_col->set_short_text( 'From' ).

        lo_col ?= lo_cols->get_column( 'BLOCK_TO' ).
        lo_col->set_long_text( 'Block End Index' ).
        lo_col->set_medium_text( 'Block To' ).
        lo_col->set_short_text( 'To' ).

        lo_col ?= lo_cols->get_column( 'MESSAGE' ).
        lo_col->set_long_text( 'Message' ).
        lo_col->set_medium_text( 'Message' ).
        lo_col->set_short_text( 'Message' ).

        lo_alv->display( ).

      CATCH cx_salv_msg INTO DATA(lx_salv).
        MESSAGE lx_salv->get_text( ) TYPE 'E'.
      CATCH cx_salv_not_found INTO DATA(lx_nf).
        MESSAGE lx_nf->get_text( ) TYPE 'I'.
    ENDTRY.
  ENDMETHOD.
ENDCLASS.

*----------------------------------------------------------------------*
* MAIN PROCESSING
*----------------------------------------------------------------------*
START-OF-SELECTION.

  DATA: lo_reader    TYPE REF TO zcl_file_reader,
        lo_scheduler TYPE REF TO zcl_job_scheduler,
        lt_lines     TYPE tt_file_lines,
        lt_blocks    TYPE zcl_job_scheduler=>tt_blocks,
        lt_results   TYPE tt_alv_results,
        lv_ok        TYPE abap_bool,
        lv_immed     TYPE abap_bool.

  lo_reader = NEW zcl_file_reader( ).
  lo_reader->read_file(
    EXPORTING iv_filepath = p_file
    IMPORTING et_lines    = lt_lines
    RECEIVING rv_ok       = lv_ok
  ).

  IF lv_ok = abap_false OR lt_lines IS INITIAL.
    MESSAGE 'No valid lines found in the uploaded file. Processing aborted.' TYPE 'E'.
  ENDIF.

  lv_immed = COND #( WHEN p_immed = 'X' THEN abap_true ELSE abap_false ).

  lo_scheduler = NEW zcl_job_scheduler(
    iv_block_size  = p_blksz
    iv_sched_immed = lv_immed
    iv_sched_date  = p_date
    iv_sched_time  = p_time
  ).

  lt_blocks = lo_scheduler->build_blocks( lt_lines ).

  IF lt_blocks IS INITIAL.
    MESSAGE 'No blocks could be built from the file content.' TYPE 'E'.
  ENDIF.

  lo_scheduler->schedule_all(
    EXPORTING it_blocks  = lt_blocks
    IMPORTING et_results = lt_results
  ).

  zcl_alv_display=>display( lt_results ).
