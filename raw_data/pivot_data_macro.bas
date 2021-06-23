Sub extract_data_from_pivot()
'
' extract_data_from_pivot Macro
' Extracts pivot underlying data for tables I and II (description below) and saves new sheet as .csv file
'

'
    '  Table I - Sales of oil derivative fuels by UF and product
    
    Sheets("Plan1").Select
    Range("C49").Select
    ActiveSheet.PivotTables("Tabela dinâmica1").PivotFields("UN. DA FEDERAÇÃO"). _
        Orientation = xlHidden
    Range("C50").Select
    ActiveSheet.PivotTables("Tabela dinâmica1").PivotFields("PRODUTO").Orientation _
        = xlHidden
    Range("C52").Select
    ActiveSheet.PivotTables("Tabela dinâmica1").PivotFields("ANO").Orientation = _
        xlHidden
    Range("C65").Select
    Selection.ShowDetail = True
    
    ' Table II - Sales of diesel by UF and type
    
    Sheets("Plan1").Select
    Range("C129").Select
    ActiveSheet.PivotTables("Tabela dinâmica3").PivotFields("UN. DA FEDERAÇÃO"). _
        Orientation = xlHidden
    Range("C130").Select
    ActiveSheet.PivotTables("Tabela dinâmica3").PivotFields("PRODUTO").Orientation _
        = xlHidden
    Range("C132").Select
    ActiveSheet.PivotTables("Tabela dinâmica3").PivotFields("ANO").Orientation = _
        xlHidden
    Range("C145").Select
    Selection.ShowDetail = True

    ' Exports table I as .cvs

    Sheets("Planilha1").Select
    Dim relativePath As String
    relativePath = ThisWorkbook.Path & Application.PathSeparator & "oil-derivative-fuels-by-uf-and-product.csv"
    ActiveWorkbook.SaveAs Filename:= _
        relativePath _
        , FileFormat:=xlCSV, CreateBackup:=False

    ' Exports table I as .cvs

    Sheets("Planilha2").Select
    relativePath = ThisWorkbook.Path & Application.PathSeparator & "diesel-by-uf-and-type.csv"
    ActiveWorkbook.SaveAs Filename:= _
        relativePath _
        , FileFormat:=xlCSV, CreateBackup:=False


    ActiveWorkbook.Close SaveChanges:=False

End Sub
