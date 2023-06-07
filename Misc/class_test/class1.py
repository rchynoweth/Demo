
class Class1(): 
    
    @staticmethod
    def return_x(x):
        return x 
    
    def return_y(y):
        return y
    
    def return_z(self, z):
        self.a = 'b'
        return z 
    
    def return_zz(z):
        return return_z(z)