class Tools:
	def __init__(self,file_name,mode):
		self.file_name = file_name
		self.mode = mode

	def string_write(self,str):
		try:
			with open(self.file_name,self.mode) as outfile:
				outfile.write(str)
		except FileNotFoundError:
			msg = "Sorry, the file "+ filename + "does not exist."
			print(msg)	 
	def dataframe_write(self,df,index,header):
		try:
			df.to_csv(self.file_name, mode=self.mode, index=index, header=header)
		except:
			msg = "Sorry, dataframe has errors"
			print(msg)	 
