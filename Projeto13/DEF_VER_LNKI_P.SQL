select
    TABLE_ID || '#;' ||
    JOB_ID || '#;' ||
    CONDITION || '#;' ||
    ODATE || '#;' ||
    AND_OR || '#;' ||
    PARENTHESES || '#;' ||
    ORDER_ || '#;' ||
    ISN || '#;' ||
    VERSION_OPCODE || '#;' ||
    IS_CURRENT_VERSION || '#;' ||
    VERSION_SERIAL
FROM EMUSR98.DEF_VER_LNKI_P
where IS_CURRENT_VERSION='Y'
