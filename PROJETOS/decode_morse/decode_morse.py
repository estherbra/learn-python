'''
O Código Morse é um sistema de representação de letras, algarismos e sinais de pontuação através
de um sinal codificado enviado de modo intermitente. Foi desenvolvido por Samuel Morse em 1837, 
criador do telégrafo elétrico, dispositivo que utiliza correntes elétricas para controlar eletroímãs 
que atuam na emissão e na recepção de sinais. 
O script tem a finalidade de decifrar uma mensagem em código morse e salvá-la em texto claro.
'''

import os
import datetime
import pandas as pd
from config import file_path, dict_morse

def decode_morse(msg):
    """
    Decodificando uma mensagem em código Morse, seguindo os critérios:
      - As letras estão separadas por um espaço simples
      - As palavras estão separadas por dois espaços
    """
    words = msg.split("  ")
    decoded_words = []

    for word in words:
        letters = word.split(" ")
        decoded_word = []
        for letter in letters:
            if letter in dict_morse:
                decoded_word.append(dict_morse[letter])
            else:
                decoded_word.append('?') 
        decoded_words.append("".join(decoded_word))

    return " ".join(decoded_words)

def save_clear_msg_csv_hdr(morse_code, msg_claro):
    """
    Salvando a mensagem em Morse e o texto decodificado em um CSV com a data e hora.
    """
    now = datetime.datetime.now()
    df = pd.DataFrame([[morse_code, msg_claro, now]], columns=["codigo_morse", "mensagem_decodificada", "datetime"])
    
    # Verificando se o CSV já existe
    hdr = not os.path.exists(file_path)
    
    # Salvando
    df.to_csv(file_path, mode="a", index=False, header=hdr)

if __name__ == "__main__":

    tres_frases = [
        ".... . .-.. .-.. ---  .-- --- .-. .-.. -.. -.-.--",
        ". ... - ..- -.. .- -. -.. ---  .--. -.-- - .... --- -.",
        "... --- ...  .--. -.-- - .... --- -."
    ]

    for i, frase in enumerate(tres_frases, 1):
        print(f"Decodificando frase {i}:")
        msg_claro = decode_morse(frase)
        print(f"Mensagem decodificada: {msg_claro}")
        
        #Salvando no CSV o código Morse e a mensagem decodificada
        save_clear_msg_csv_hdr(frase, msg_claro)
        print(f"Mensagem e código Morse salvos no CSV.\n")
