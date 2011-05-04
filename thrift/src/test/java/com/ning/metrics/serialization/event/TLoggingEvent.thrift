struct TLoggingEvent   {
    1:i64 eventDate,
    2:string level,
    3:string message,
    4:string loggername,
    5:string locationFilename,
    6:string locationMethodName,
    7:string locationClassName,
    8:string locationLineNumber,
    9:string ndc,
    10:string threadName,
    11:string throwableLocalizedMessage,
    12:string throwableMessage,
    13:string stacktrace,
    14:string coreIp,
    15:string coreHostname,
    16:string coreType,
    17:string trace
}
