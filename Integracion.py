from thefuzz import fuzz, process
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'

#Rutas absolutas
dfCompletoSalary = pd.read_csv("C:\\Users\\titob\\Downloads\\Salary.csv")
dfCompleto = pd.read_csv("C:\\Users\\titob\\Downloads\\CostOfLiving.csv")
dfIndexFix = pd.read_csv("C:\\Users\\titob\\Downloads\\CostOfNewYorkInUSD.csv", usecols=range(1,5))

#Solución heterogeneidad de datos y filtrado de ciudades USA (transformación 1).
dfList=dfCompleto["City"].values.tolist()
dfListScored=process.extract("United States", dfList, limit=None)
res=[e for (e, score) in dfListScored if score >= 90]
rslt_df = dfCompleto[dfCompleto['City'].isin(res)]
for row in rslt_df:
	rslt_df['City'] = rslt_df['City'].replace(", United States","",regex=True)
rslt_df.reset_index()


#Conversión de index a dólar (Transformación 1)
dfIndexFix["Cost of Living avg"] = pd.to_numeric(dfIndexFix["Cost of Living avg"])
dfIndexFix["Rent avg"] = pd.to_numeric(dfIndexFix["Rent avg"])
dfIndexFix["Cost of Living Plus Rent avg"] = pd.to_numeric(dfIndexFix["Cost of Living Plus Rent avg"])
dfIndexFix["Local Purchasing Power avg"] = pd.to_numeric(dfIndexFix["Local Purchasing Power avg"])
rslt_df["Cost of Living Index"] = pd.to_numeric(rslt_df["Cost of Living Index"])
rslt_df["Rent Index"] = pd.to_numeric(rslt_df["Rent Index"])
rslt_df["Cost of Living Plus Rent Index"] = pd.to_numeric(rslt_df["Cost of Living Plus Rent Index"])
rslt_df["Local Purchasing Power Index"] = pd.to_numeric(rslt_df["Local Purchasing Power Index"])
for ind in rslt_df.index:
	aux=round((rslt_df["Cost of Living Index"][ind] * dfIndexFix.loc[0]["Cost of Living avg"])/100, 1)
	rslt_df["Cost of Living Index"] = rslt_df["Cost of Living Index"].replace(rslt_df["Cost of Living Index"][ind],aux)

	aux=round((rslt_df["Rent Index"][ind] * dfIndexFix.loc[0]["Rent avg"])/100, 1)
	rslt_df["Rent Index"] = rslt_df["Rent Index"].replace(rslt_df["Rent Index"][ind],aux)

	aux=round((rslt_df["Cost of Living Plus Rent Index"][ind] * dfIndexFix.loc[0]["Cost of Living Plus Rent avg"])/100, 1)
	rslt_df["Cost of Living Plus Rent Index"] = rslt_df["Cost of Living Plus Rent Index"].replace(rslt_df["Cost of Living Plus Rent Index"][ind],aux)

	aux=round((rslt_df["Local Purchasing Power Index"][ind] * dfIndexFix.loc[0]["Local Purchasing Power avg"])/100, 1)
	rslt_df["Local Purchasing Power Index"] = rslt_df["Local Purchasing Power Index"].replace(rslt_df["Local Purchasing Power Index"][ind],aux)

rslt_df.rename(columns = {"Cost of Living Index": "Cost of Living avg", 
	"Rent Index":"Rent avg","Cost of Living Plus Rent Index":"Cost of Living Plus Rent avg", 
	"Local Purchasing Power Index":"Local Purchasing Power avg"}, inplace = True)
rslt_df.reset_index()

#Matching de composiciones.
dfListSalary=dfCompletoSalary["Metro"].values.tolist()
rslt_dfListCity=rslt_df["City"].values.tolist()
dfListSalaryScored=[]
for i in rslt_dfListCity:
	score=process.extractOne(i, dfListSalary, scorer=fuzz.token_set_ratio)
	dfListSalaryScored.append((i,score))

#Join 
metroaux=[]
for i in range(len(dfListSalaryScored)):
	if dfListSalaryScored[i][1][1]==100  :
		metroaux.append(dfListSalaryScored[i][1][0])
	else :
		metroaux.append("")

rslt_df["Metro"]=metroaux
result_df=dfCompletoSalary.merge(rslt_df, left_on='Metro', right_on='Metro')
result_df["Mean Software Developer Salary (adjusted)"] = pd.to_numeric(result_df["Mean Software Developer Salary (adjusted)"])
result_df["Mean Software Developer Salary (unadjusted)"] = pd.to_numeric(result_df["Mean Software Developer Salary (unadjusted)"])
result_df["Mean Unadjusted Salary (all occupations)"] = pd.to_numeric(result_df["Mean Unadjusted Salary (all occupations)"])
result_df["Number of Software Developer Jobs"] = pd.to_numeric(result_df["Number of Software Developer Jobs"])
result_df["Median Home Price"] = pd.to_numeric(result_df["Median Home Price"])
del result_df["Rank"]

#DATASET RESULTANTE EXPORTADO COMO .CSV
result_df.to_csv('result.csv')





