import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

print("*************************************")
print("* Bem vindo! Sequência de Fibonacci *")
print("*************************************")
print(" ")

try:
    valor_maximo = int(input("Digite o número máximo que a Sequência de Fibonacci deve tratar:\n "))
    print(f"O {valor_maximo}-ésimo número da sequência de Fibonacci é: {fibonacci(valor_maximo)}")
except ValueError:
    print("Digite um número válido!")

print("Fim do programa!")
