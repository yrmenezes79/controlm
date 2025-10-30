select
    TABLE_ID || '#;' ||
    JOB_ID || '#;' ||
    RESOURCE || '#;' ||
    QUANT || '#;' ||
    ISN || '#;' ||
    HANDLE_RES_OK || '#;' ||
    HANDLE_RES_NOTOK || '#;' ||
    VERSION_OPCODE || '#;' ||
    IS_CURRENT_VERSION || '#;' ||
    VERSION_SERIAL
FROM EMUSR98.DEF_VER_LNKI_Q
where IS_CURRENT_VERSION='Y'
