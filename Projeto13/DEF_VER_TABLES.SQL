SELECT
    TABLE_ID || '#;' ||
    DATA_CENTER || '#;' ||
    SCHED_TABLE || '#;' ||
    USED_BY || '#;' ||
    USED_BY_CODE || '#;' ||
    MODIFIED || '#;' ||
    LAST_UPLOAD || '#;' ||
    DSN || '#;' ||
    USER_DAILY || '#;' ||
    CHECKSUM || '#;' ||
    REAL_TABLEID || '#;' ||
    TABLE_STATUS || '#;' ||
    TBL_DELETION_TIMESTAMP || '#;' ||
    TBL_DELETION_USER || '#;' ||
    TBL_DELETION_HOST || '#;' ||
    LAST_UPDATED || '#;' ||
    TABLE_TYPE || '#;' ||
    LAST_UPDATED_USER || '#;' ||
    SYNC_STATE || '#;' ||
    SYNC_MSG || '#;' ||
    LAST_EVENT_TIMESTAMP || '#;' ||
    SITE_STD_ID || '#;' ||
    DEF_CD || '#;' ||
    STRICTNESS
FROM EKUSR98.DEF_VER_TABLES
