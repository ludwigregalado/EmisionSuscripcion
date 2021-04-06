source("../Metas Refacciones/customizedHDI.R")# Loading several customized functions that might be useful
necessaryPkg(c("RODBC", "lubridate","dplyr","ggplot2"))# Loading requiered packages

DWH <- odbcDriverConnect(readLines("connection_R.sql"))
# query <- paste(scan("OTs.sql", what = "", sep = "\n", blank.lines.skip = FALSE), collapse = "\n")# Reading a sql file wich contains the query
query <- paste(readLines("OTs.sql"), collapse = "\n")
baseDatos <- sqlQuery(DWH, query)# Quering data from Data Ware House
close(DWH)

# baseDatos[,9:11] <- sapply(baseDatos[,9:11],as.Date)

# baseDatos <- baseDatos[complete.cases(baseDatos),]# Removing cases with NA

baseDatos <- baseDatos %>% 
              mutate(tiempoAtencion = difftime(TiempoEmision, TiempoAltaOt, units = "hours"))# Calculating elapsed time in hours

baseDatos %>% ggplot(aes(x = tiempoAtencion, color = TipoPuesto, fill = TipoPuesto))+
                      geom_histogram(binwidth = 5)+
                      facet_wrap(~TipoPuesto)+
                      coord_cartesian(xlim = c(0,300))+
                      labs(x = "Tiempo de atención (horas)", y = "Número de casos")+
                      ggtitle("Análisis por tipo de puesto")+
                      theme_dark()

baseDatos %>% ggplot(aes(x = TipoPuesto, y = tiempoAtencion, color = TipoPuesto, fill = TipoPuesto))+
                      geom_violin()+
                      coord_cartesian(ylim = c(0,300))+
                      labs(x = "Puesto", y = "Tiempo de atención (horas)")+
                      ggtitle("Análisis por tipo de puesto")+
                      theme_dark()

baseDatos %>% ggplot(aes(x = tiempoAtencion, color = TipoPuesto, fill = TipoPuesto))+
                      geom_boxplot()+
                      coord_cartesian(xlim = c(0,300))+
                      facet_wrap(~IdTipoDocumento)+
                      labs(x = "Tiempo de atención (horas)")+
                      ggtitle("Análisis por tipo de documento")+
                      theme_dark()

baseDatos %>% ggplot(aes(x = tiempoAtencion, color = TipoPuesto, fill = TipoPuesto))+
                      geom_histogram(binwidth = 5)+
                      facet_wrap(~IdTipoDocumento)+
                      coord_cartesian(xlim = c(0,300))+
                      labs(x = "Tiempo de atención (horas)", y = "Número de casos")+
                      ggtitle("Análisis por tipo de documento")+scale_y_log10()+
                      theme_dark()

baseDatos %>% ggplot(aes(x = tiempoAtencion, color = IdTipoDocumento, fill = IdTipoDocumento))+
                      geom_boxplot()+
                      facet_wrap(~TipoPuesto)+
                      coord_cartesian(xlim = c(0,300))+
                      labs(x = "Tiempo de atención (horas)")+
                      ggtitle("Análisis por tipo de documento")+
                      theme_dark()

# duracion <- baseDatos$TiempoEstatus-baseDatos$TiempoAltaOt