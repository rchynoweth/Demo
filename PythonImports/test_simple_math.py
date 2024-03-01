from simple_math import SimpleMath

def test_multiply():
    math = SimpleMath()
    result = math.multiply(3, 4)
    assert result == 12

def test_is_even():
    math = SimpleMath()
    assert math.is_even(6) == True
    assert math.is_even(7) == False
