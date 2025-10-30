select
    TABLE_ID || '#;' ||
    JOB_ID || '#;' ||
    CONDITION || '#;' ||
    ODATE || '#;' ||
    SIGN || '#;' ||
    ISN || '#;' ||
    VERSION_OPCODE || '#;' ||
    IS_CURRENT_VERSION || '#;' ||
    VERSION_SERIAL
FROM EMUSR98.DEF_VER_LNKO_P
where IS_CURRENT_VERSION='Y'
