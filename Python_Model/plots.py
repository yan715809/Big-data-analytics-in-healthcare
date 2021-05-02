import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns



def plot_learning_curves(train_losses, valid_losses, train_accuracies, valid_accuracies):

	plt.figure(1)
	plt.plot(np.arange(len(train_losses)), train_losses, label='Training Loss')    
	plt.plot(np.arange(len(valid_losses)), valid_losses, label='Validation Loss')
	plt.xlabel('Epoch')
	plt.ylabel('Loss')
	plt.title('Loss Curve')
	plt.legend(loc='best')
	
    
	plt.figure(2)
	plt.plot(np.arange(len(train_accuracies)), train_accuracies, label='Training Accuracy')     
	plt.plot(np.arange(len(valid_accuracies)), valid_accuracies, label='Validation Accuracy')
	plt.xlabel('Epoch')    
	plt.ylabel('Accuracy')    
	plt.title('Accuracy Curve')    
	plt.legend(loc='best')
    
	plt.show()
    

    
def plot_confusion_matrix(results, class_names):

	n_classes = len(class_names)
	conf_matrix = np.zeros((n_classes, n_classes))    
	for i in range(n_classes):
		i_total = np.sum([j[0]==i for j in results])
		if i_total ==0 :
			conf_matrix[i, :]==0
		else:
			for l in range(n_classes):
				l_percent = np.sum([j[1]==l and j[0]==i for j in results])/i_total
				conf_matrix[i,l]=l_percent    
    
	ax=sns.heatmap(conf_matrix, annot=True, cmap='Blues', xticklabels=class_names, 
            yticklabels=class_names)
	ax.set(ylabel='True', xlabel='Predicted', title='Normalized Confusion Matrix')
	plt.xticks(rotation=45)
	plt.show()    
	    
	    
	    
    
    
    
    
    
    
    
    
    
    
