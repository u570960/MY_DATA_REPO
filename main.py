class Phrase:
    def __init__ (self,phrase):
        self.mots = phrase.split()
        
    def upper(self):
        self.mots = [m.upper() for m in self.mots]
    
    def __str__(self):
        return "\n".join(self.mots)

if test1: 
	<bloc 1>
elif test2:
	<bloc2>
else:
	<blocN>
while a: 
	a.pop()
	print(a)
	
while True:
	s = input('Quelle est votre question ? \n')
	if 'aucune' in s:
		break
	
while True:
	s = input('Quelle est votre question ? \n')
	if 'aucune' in s:
		break
	
