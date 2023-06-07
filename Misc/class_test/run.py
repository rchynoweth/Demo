from class1 import Class1
from class2 import Class2 

c1 = Class1()
c2 = Class2()


c1.return_x('class1_return')

c2.return_x('class2_return')

class1_return_x_func = Class1.return_x
class1_return_x_func('class1_return')


c1.return_z('class1_return')
Class1.return_z(Class1, 'class_return')