#! /usr/bin/env python

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib_venn import venn2 # type: ignore
import matplotlib.patches as mpatches

# global
workload = ""

def read_key_stats_file(file_name: str, col_names: list):
    df = pd.read_csv(file_name, header = None, names = col_names)
    # print(df)
    return df

"""
绘制频率曲线
"""
def plot_frequency_line(input_file_name: str, output_file_name: str, partial = False):
    df = read_key_stats_file(input_file_name, ["Keys", "Frequencies"])
    # 截取前 N% 数据
    if (partial):
        total_rows = len(df)
        n = int(total_rows * 0.0001)
        n = max(1, n)
        keys = df["Keys"].values[:n]
        frequencies = df["Frequencies"].values[:n]
    else:
        keys = df["Keys"].values
        frequencies = df["Frequencies"].values
    plt.figure(figsize = (12, 6))
    plt.plot(keys, frequencies, linestyle = '-', color = 'b')
    plt.xticks([])
    plt.xlabel("Key Space", fontsize = 12)
    plt.ylabel("Frequency", fontsize = 12)
    if (partial):
        plt.title("Frequency Distribution of Keys (Top-0.01%), Zipfian", fontsize = 14)
    else:
        plt.title("Frequency Distribution of Keys", fontsize = 14)
    plt.grid(True, linestyle = '--', alpha = 0.7)
    ### 标记峰值 ###
    max_idx = np.argmax(frequencies)  # 找到全局最大值的索引
    max_key = keys[max_idx]
    max_freq = frequencies[max_idx]
    plt.scatter(max_key, max_freq, color = 'red', s = 60)
    plt.axhline(
       y = max_freq,
       linestyle = '--',
       color = 'grey',
       alpha = 0.7
    )
    plt.text(
        x = -0.01,
        y = max_freq, 
        s = f'{max_freq}', 
        transform = plt.gca().get_yaxis_transform(),
        ha = 'right',  # 文本右对齐
        va = 'center',  # 垂直居中
        fontsize = 10
    )
    plt.savefig(output_file_name)

"""
计算召回率、准确率
"""
def calculate_hotkeys_coverage(groundtruth_file_name: str, result_file_name: str, algo_name: str):
    df_gt = read_key_stats_file(groundtruth_file_name, ["Keys"])
    df_result = read_key_stats_file(result_file_name, ["Keys"])
    # 覆盖率表示有多少热键能被识别
    recall = (df_result["Keys"].isin(df_gt["Keys"]).sum() / len(df_gt)) * 100
    # 查准率表示被识别为热键的正确率
    precision = df_result["Keys"].isin(df_gt["Keys"]).mean() * 100

    print(f"{algo_name}: \n - Recall: {recall:.2f}%\n - Precision: {precision:.2f}%")
    return recall, precision

color_yellow = '#FFC107'
color_red = '#F44336'
color_green = '#4CAF50'

"""
绘制堆叠分组条形图
"""
def plot_stacked_barchart(groundtruth_file_name: str, algorithms: list):
    df_gt = read_key_stats_file(groundtruth_file_name, ["Keys"])
    gt_total = len(df_gt)

    data = {'hit': [], 'miss': [], 'false': [], 'Total': gt_total}
    for algo in algorithms:
        result_file = f"./{workload}_hotkeys_{algo}.csv"
        df_result = read_key_stats_file(result_file, ["Keys"])
        # 正确识别的热键
        hit = df_result["Keys"].isin(df_gt["Keys"]).sum()
        # 未识别的真实热键
        miss = gt_total - hit
        # 错误识别的热键
        false = len(df_result) - hit
        data['hit'].append(hit)
        data['miss'].append(miss)
        data['false'].append(false)
    
    plt.figure(figsize=(12, 8)) 
    x = np.arange(len(algorithms))
    width = 0.7
    # 颜色设置
    colors = {'hit': color_green, 'miss': color_yellow, 'false': color_red}
    # 从上到下顺序：false、miss、hit
    # 1. hit part
    p1 = plt.bar(x, data['hit'], width, color = colors['hit'], alpha = 0.7)
    # 2. miss part
    p2 = plt.bar(x, data['miss'], width, bottom = data['hit'], color = colors['miss'], alpha = 0.7)
    # 3. false part
    bottom_part = [a + b for a, b in zip(data['hit'], data['miss'])]
    p3 = plt.bar(x, data['false'], width, bottom = bottom_part, color = colors['false'], alpha = 0.7)
    # 添加 tags
    for i, algo in enumerate(algorithms):
        total_stack = data['hit'][i] + data['miss'][i] + data['false'][i]
        # add tag at false part
        plt.text(i, bottom_part[i] + data['false'][i]/2, 
                 f"FP: {data['false'][i]}\n({data['false'][i]/total_stack:.1%})", 
                 ha='center', va='center', color='black', fontsize=12)
        # add tag at miss part
        plt.text(i, data['hit'][i] + data['miss'][i]/2, 
                 f"FN: {data['miss'][i]}\n({data['miss'][i]/gt_total:.1%})", 
                 ha='center', va='center', color='black', fontsize=12)
        # add tag at hit part
        plt.text(i, data['hit'][i]/2, 
                 f"TP: {data['hit'][i]}\n({data['hit'][i]/gt_total:.1%})", 
                 ha='center', va='center', color='black', fontsize=12)
        plt.text(i, total_stack + 0.1 * gt_total, 
                 f"Total: {total_stack}", 
                 ha='center', va='bottom', fontsize=12, fontweight='bold')
    # 添加标题和标签
    plt.title('Stacked Comparison of Different Algorithms', fontsize = 16, pad = 20)
    # plt.xlabel('Algorithms', fontsize = 12, labelpad = 10)
    plt.ylabel('Key Count', fontsize = 12, labelpad = 10)
    plt.xticks(x, algorithms, fontsize = 12)
    plt.ylim(0, max([sum(values) for values in zip(data['hit'], data['miss'], data['false'])]) * 1.2)
    legend_labels = [
        mpatches.Patch(color=colors['hit'], alpha = 0.7, label=f'Hit: Correctly Identified (TP)'),
        mpatches.Patch(color=colors['miss'], alpha = 0.7, label=f'Miss: Not Identified (FN)'),
        mpatches.Patch(color=colors['false'], alpha = 0.7, label=f'False: Wrongly Identified (FP)')
    ]
    plt.legend(handles=legend_labels, loc='upper left', fontsize = 14)
    plt.grid(axis='y', alpha=0.3)
    plt.axhline(y=gt_total, color='gray', linestyle='--', alpha=0.7)
    plt.text(
        x = -0.01,
        y = gt_total,
        s = f'{gt_total}', 
        transform = plt.gca().get_yaxis_transform(),
        ha = 'right',
        va = 'center',
        fontsize = 10
    )
    plt.tight_layout()
    plt.savefig(f'{workload}_stacked_barchart_comparison.png', dpi=300)
    print("Stacked Barchart is generated")


"""
绘制韦恩图展示集合关系
"""
def plot_venn_diagram(groundtruth_file: str, result_file: str, algo_name: str):
    df_gt = read_key_stats_file(groundtruth_file, ["Keys"])
    df_result = read_key_stats_file(result_file, ["Keys"])
    set_gt = set(df_gt["Keys"])
    set_result = set(df_result["Keys"])
    plt.figure(figsize=(8, 8))
    left_color = color_yellow
    right_color = color_red
    intersection_color = color_green
    venn = venn2(
        subsets=(set_gt, set_result),
        set_labels=('Groundtruth Keys', f'{algo_name} Results'),
        set_colors=(left_color, right_color),
        alpha=0.7
    )
    # 中间交集区域
    if venn.get_label_by_id('11'):
        venn.get_patch_by_id('11').set_color(intersection_color)
    plt.title(f'Key Coverage: {algo_name} vs Groundtruth', fontsize=14)
    for text in venn.set_labels:
        text.set_fontsize(12)
    for text in venn.subset_labels:
        if text: text.set_fontsize(12)
    plt.savefig(f'{workload}_venn_{algo_name}.png', dpi=300)
    print(f"{algo}'s Venn Figure is generated")


if __name__ == '__main__':
    # Check args
    if (len(sys.argv) < 2):
        print("Error, please input workload prefix!")
        sys.exit()
    # 识别命令行参数为 workload 前缀
    # 该前缀必须与 workload 相同，比如 workloada.spec --> workloada
    workload = str(sys.argv[1])
    print("Workload prefix:", workload)

    algorithms = ["lru", "lfu", "lruk", "window", "sketch_window", "w_tinylfu", "lirs"]
    
    for algo in algorithms:
        # 计算召回、准确率
        recall, precision = calculate_hotkeys_coverage(
            f"./{workload}_key_stats_hotkeys.csv", 
            f"./{workload}_hotkeys_{algo}.csv", 
            algo
        )
        # 绘制韦恩图查看交集
        plot_venn_diagram(
            f"./{workload}_key_stats_hotkeys.csv",
            f"./{workload}_hotkeys_{algo}.csv",
            algo
        )
    # 绘制堆叠条形图
    plot_stacked_barchart(f"./{workload}_key_stats_hotkeys.csv", algorithms)
    
    plot_frequency_line(f"./{workload}_key_stats_dict_ordered.csv", f"{workload}_key_frequency.png", False)
    plot_frequency_line(f"./{workload}_key_stats_descend.csv", f"{workload}_frequency_descend.png", False)