select
    TABLE_ID || '#;' ||
    JOB_ID || '#;' ||
    WHEN_COND || '#;' ||
    SHOUT_TIME || '#;' ||
    URGENCY || '#;' ||
    DESTINATION || '#;' ||
    MESSAGE || '#;' ||
    ISN || '#;' ||
    SHOUT_DAYSOFFSET || '#;' ||
    VERSION_OPCODE || '#;' ||
    IS_CURRENT_VERSION || '#;' ||
    VERSION_SERIAL
FROM EMUSR98.DEF_VER_SHOUT
where IS_CURRENT_VERSION='Y'
