select
    TABLE_ID || '#;' ||
    JOB_ID || '#;' ||
    URGENCY || '#;' ||
    MESSAGE || '#;' ||
    DESTINATION || '#;' ||
    IF_TEXT || '#;' ||
    DO_NO || '#;' ||
    ISN || '#;' ||
    VERSION_OPCODE || ';#;' ||
    IS_CURRENT_VERSION || ';#;' ||
    VERSION_SERIAL
FROM EKUSR98.DEF_VER_DO_SHOUT
WHERE IS_CURRENT_VERSION='Y'
