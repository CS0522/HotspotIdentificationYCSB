#! /usr/bin/env python

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def read_key_stats_file(file_name: str, col_names: list):
    df = pd.read_csv(file_name, header = None, names = col_names)
    # print(df)
    return df

def plot_pdf(input_file_name: str, output_file_name: str):
    df = read_key_stats_file(input_file_name, ["Keys", "Frequencies"])
    keys = df["Keys"].values
    frequencies = df["Frequencies"].values

    plt.savefig(output_file_name)


def plot_frequency_line(input_file_name: str, output_file_name: str):
    df = read_key_stats_file(input_file_name, ["Keys", "Frequencies"])
    keys = df["Keys"].values
    frequencies = df["Frequencies"].values
    plt.figure(figsize = (12, 6))
    plt.plot(keys, frequencies, linestyle = '-', color = 'b')
    plt.xticks([])
    plt.xlabel("Key Space", fontsize = 12)
    plt.ylabel("Frequency", fontsize = 12)
    plt.title("Frequency Distribution of Keys", fontsize = 14)
    plt.grid(True, linestyle = '--', alpha = 0.7)
    ### 标记峰值 ###
    max_indices = np.where(frequencies >= 500)[0]
    for idx in max_indices:
        plt.scatter(keys[idx], frequencies[idx], color = 'red', s = 60)
        plt.axhline(
           y = frequencies[idx],
           linestyle = '--',
           color = 'grey',
           alpha = 0.7
        )
        plt.text(
            x = -0.01,
            y = frequencies[idx], 
            s = f'{frequencies[idx]}', 
            transform = plt.gca().get_yaxis_transform(),
            ha = 'right',  # 文本右对齐
            va = 'center',  # 垂直居中
            fontsize = 10
        )
    plt.savefig(output_file_name)

def calculate_hotkeys_coverage(groudtruth_file_name: str, result_file_name: str, algo_name: str):
    df_groundtruth = read_key_stats_file(groudtruth_file_name, ["Keys"])
    df_result = read_key_stats_file(result_file_name, ["Keys"])
    coverage = df_result["Keys"].isin(df_groundtruth["Keys"]).mean() * 100
    # 返回覆盖率结果
    print(f"{algo_name}: \n - Hot Keys identificated ratio: {coverage:.2f}%")
    return coverage


if __name__ == '__main__':
    # plot_frequency_line("./key_stats_dict_ordered.csv", "./fig_key_frequency.png")
    # plot_frequency_line("./key_stats_descend.csv", "./fig_frequency_descend.png")
    algorithms = ["lruk", "window", "sketch"]
    for algo in algorithms:
        calculate_hotkeys_coverage("./key_stats_hotkeys.csv", f"./hotkeys_{algo}.csv", algo)
    